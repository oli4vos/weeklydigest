"""Weekly digest generation service (non-AI)."""
from __future__ import annotations

import logging
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from textwrap import shorten
from urllib.parse import urlparse
from typing import Dict, List, Optional, Tuple

from app.db import get_session
from app.models import RawMessage, Resource, WeeklyReport
from app.repositories.weekly_report_repository import WeeklyReportRepository


class DigestService:
    """Builds weekly summaries from deterministic extraction data."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def generate_report(
        self,
        *,
        user_id: int,
        start: date,
        end: date,
        force: bool = False,
    ) -> WeeklyReport:
        """Generate (and persist) a digest for the given period."""
        with get_session() as session:
            existing = WeeklyReportRepository.find_in_range(session, user_id, start, end)
            if existing and not force:
                raise ValueError(
                    f"Weekly report already exists for user {user_id} between {start} and {end}. "
                    "Use --force to regenerate."
                )

            messages = self._load_messages(session, user_id, start, end)
            resources = self._load_resources(session, user_id, start, end)
            previous = WeeklyReportRepository.latest_for_user(session, user_id)

            report_data = self._build_payload(messages, resources, previous, start, end)
            report = WeeklyReportRepository.create(
                session,
                user_id=user_id,
                week_start=start,
                week_end=end,
                data=report_data,
            )
            session.commit()
            self.logger.info(
                "Generated weekly report %s for user %s covering %s - %s",
                report.id,
                user_id,
                start,
                end,
            )
            return report

    def _load_messages(
        self, session, user_id: int, start: date, end: date
    ) -> List[RawMessage]:
        start_dt = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
        end_dt = datetime.combine(end, datetime.max.time(), tzinfo=timezone.utc)
        stmt = (
            session.query(RawMessage)
            .filter(
                RawMessage.user_id == user_id,
                RawMessage.received_at.between(start_dt, end_dt),
            )
            .order_by(RawMessage.received_at.asc())
        )
        return stmt.all()

    def _load_resources(
        self, session, user_id: int, start: date, end: date
    ) -> List[Resource]:
        start_dt = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
        end_dt = datetime.combine(end, datetime.max.time(), tzinfo=timezone.utc)
        stmt = (
            session.query(Resource)
            .join(RawMessage)
            .filter(
                RawMessage.user_id == user_id,
                RawMessage.received_at.between(start_dt, end_dt),
            )
            .order_by(Resource.id.asc())
        )
        return stmt.all()

    def _build_payload(
        self,
        messages: List[RawMessage],
        resources: List[Resource],
        previous: Optional[WeeklyReport],
        start: date,
        end: date,
    ) -> Dict:
        """Aggregate the raw data for storage on the WeeklyReport row."""
        message_count = len(messages)
        resource_count = len(resources)
        ideas = self._plain_text_notes(messages)
        note_count = len(ideas)
        external_resources = [
            res for res in resources if not (res.platform == "telegram" and res.content_format == "plain_text")
        ]
        platform_counts = Counter((res.platform or "unknown") for res in external_resources)
        format_counts = Counter((res.content_format or "unknown") for res in external_resources)
        domain_counts = Counter((res.domain or "unknown") for res in external_resources)

        highlights = self._build_highlights(external_resources, ideas)
        themes = self._build_themes(platform_counts, format_counts, domain_counts, note_count, len(external_resources))
        links_overview = self._links_overview(external_resources)
        actions = self._action_items(ideas, highlights, themes, links_overview)
        reflection = self._build_reflection(message_count, len(external_resources), note_count, themes, links_overview)
        meta_analysis = self._build_meta_analysis(previous, message_count, resource_count)
        intro_summary = self._intro_summary(message_count, len(external_resources), note_count)

        subject = f"Weekly Digest {start:%Y-%m-%d} - {end:%Y-%m-%d}"
        body = self._render_email_body(
            subject=subject,
            intro=intro_summary,
            highlights=highlights,
            themes=themes,
            notes=ideas,
            links=links_overview,
            actions=actions,
            reflection=reflection,
            meta_analysis=meta_analysis,
        )

        return {
            "source_message_count": message_count,
            "source_resource_count": resource_count,
            "highlights_json": highlights,
            "themes_json": themes,
            "ideas_json": ideas,
            "actions_json": actions,
            "reflection": reflection,
            "meta_analysis": meta_analysis,
            "email_subject": subject,
            "email_body": body,
            "status": "generated",
            "generated_at": datetime.now(timezone.utc),
        }

    def _build_highlights(self, resources: List[Resource], ideas: List[Dict]) -> List[Dict]:
        highlights: List[Dict] = []
        seen_note_keys = set()
        target_max = 5
        # 1) Prioritize strong own notes (literal snippets, no duplicates)
        sorted_ideas = sorted(
            ideas,
            key=lambda n: (len(n["text"]), n["received_at"]),
            reverse=True,
        )
        for note in sorted_ideas:
            key = note["text"].strip().lower()
            if not key or key in seen_note_keys:
                continue
            seen_note_keys.add(key)
            highlights.append(
                {
                    "type": "note",
                    "label": "Eigen idee",
                    "title": shorten(note["text"], width=90, placeholder="…"),
                    "detail": "Eigen notitie met directe relevantie voor je volgende stap.",
                    "why_relevant": "Omdat dit jouw eigen denkwerk is en direct omgezet kan worden naar actie.",
                }
            )
            if len(highlights) >= 2:
                break

        # 2) Add strong external resources while filtering noise and duplicates.
        seen_urls = set()
        candidates = [
            res
            for res in resources
            if self._is_strong_resource(res) and not self._is_noise_resource(res)
        ]
        candidates.sort(
            key=lambda r: (
                (r.fetched_at or datetime.min),
                len((r.description or r.extracted_text or "")[:240]),
            ),
            reverse=True,
        )
        for res in candidates:
            url_key = self._resource_dedupe_key(res)
            if url_key in seen_urls:
                continue
            seen_urls.add(url_key)
            detail_text = shorten(self._resource_importance(res), width=110, placeholder="…")
            highlights.append(
                {
                    "type": "resource",
                    "title": res.title or res.canonical_url or res.url,
                    "detail": detail_text,
                    "url": res.canonical_url or res.final_url or res.url,
                    "why_relevant": self._resource_why_relevant(res),
                }
            )
            if len(highlights) >= target_max:
                break

        # Ensure mix: if no notes but ideas exist, pull at least one literal note
        if not any(h["type"] == "note" for h in highlights) and ideas:
            note = ideas[0]
            highlights.insert(
                0,
                {
                    "type": "note",
                    "label": "Eigen idee",
                    "title": shorten(note["text"], width=90, placeholder="…"),
                    "detail": "Eigen notitie met directe relevantie voor je volgende stap.",
                    "why_relevant": "Omdat dit jouw eigen denkwerk is en direct omgezet kan worden naar actie.",
                },
            )
            highlights = highlights[:target_max]
        return highlights[:target_max]

    def _plain_text_notes(self, messages: List[RawMessage]) -> List[Dict]:
        notes = []
        for msg in messages:
            if msg.contains_link:
                continue
            notes.append(
                {
                    "received_at": msg.received_at.isoformat(timespec="minutes"),
                    "text": msg.text.strip(),
                }
            )
        return notes[:10]

    def _action_items(
        self,
        notes: List[Dict],
        highlights: List[Dict],
        themes: Dict,
        links: Dict[str, List[Dict]],
    ) -> List[str]:
        actions: List[str] = []

        if notes:
            for note in notes[:1]:
                snippet = shorten(note["text"], width=70, placeholder="…")
                actions.append(f"Werk dit idee uit in 5 bullets: '{snippet}'.")
        else:
            actions.append("Schrijf vandaag 1 eigen notitie in 5 bullets op basis van je belangrijkste bron.")

        resource_highlights = [hl for hl in highlights if hl.get("type") == "resource"]
        for hl in resource_highlights[:2]:
            actions.append(f"Lees '{hl['title']}' en noteer 3 inzichten die je deze week toepast.")
            if len(actions) >= 3:
                break

        short_ratio = themes.get("short_ratio", 0.0)
        if len(actions) < 3 and links.get("videos") and short_ratio >= 0.5:
            actions.append("Kies 1 korte video en vervang die vandaag door 1 long-form artikel over hetzelfde onderwerp.")

        if len(actions) < 3:
            actions.append("Kies 1 highlight en vat die samen in 3 concrete lessen voor morgen.")

        return actions[:3]

    def _build_reflection(
        self,
        messages: int,
        external_resources: int,
        notes: int,
        themes: Dict,
        links: Dict[str, List[Dict]],
    ) -> str:
        if messages == 0 and external_resources == 0:
            return "Geen activiteit – plan bewust tijd om ideeën en bronnen te verzamelen."

        total_inputs = notes + external_resources
        consumption_ratio = external_resources / total_inputs if total_inputs else 0
        if consumption_ratio >= 0.7:
            focus = "Je week was consumptiegedreven; veel externe input maar weinig eigen verwerking."
            lever = "pak één bron en schrijf er een eigen take bij."
        elif consumption_ratio <= 0.3:
            focus = "Je week draaide vooral om eigen denkwerk – sterk, mits je nu durft te publiceren."
            lever = "kies één idee en maak het zichtbaar."
        else:
            focus = "Er was een gezonde mix tussen externe bronnen en eigen notities."
            lever = "zorg dat minstens één link of idee eindigt in een concrete actie."

        short_ratio = themes.get("short_ratio", 0.0)
        if short_ratio >= 0.6:
            format_comment = "Veel short-form content; vertraag bewust zodat inzichten blijven hangen."
        elif themes.get("content_formats"):
            format_comment = "Je mix bevat voldoende long-form materiaal om diepte te pakken."
        else:
            format_comment = "Weinig gelabelde formats – log meer context zodat je patronen ziet."

        if short_ratio >= 0.5 and notes == 0:
            observation = "Duidelijke observatie: je consumeert veel korte content maar schrijft weinig."
        elif notes > external_resources:
            observation = "Duidelijke observatie: je schrijft meer eigen ideeën dan dat je links consumeert."
        else:
            observation = "Duidelijke observatie: je input is vooral consumptie, met beperkte eigen uitwerking."

        return f"{focus} {format_comment} {observation} Hefboom voor komende week: {lever}"

    def _build_meta_analysis(
        self,
        previous: Optional[WeeklyReport],
        current_messages: int,
        current_resources: int,
    ) -> str:
        if not previous:
            return "Eerste rapport voor deze gebruiker; toekomstige weken vergelijken we automatisch met eerdere totals."
        delta_msg = current_messages - (previous.source_message_count or 0)
        delta_res = current_resources - (previous.source_resource_count or 0)
        msg_trend = _trend_text(delta_msg, "berichten")
        res_trend = _trend_text(delta_res, "bronnen")
        return f"Ten opzichte van vorige week {msg_trend} en {res_trend}."

    def _intro_summary(
        self, message_count: int, external_count: int, note_count: int
    ) -> str:
        total_inputs = external_count + note_count
        if total_inputs == 0:
            return "Geen nieuwe input deze week. Kies een moment om zowel een link als een eigen notitie vast te leggen."
        note_share = note_count / total_inputs if total_inputs else 0
        if note_share >= 0.6:
            observation = "Eigen ideeën domineerden – houd dit vast door één notitie om te zetten in actie."
        elif note_share <= 0.25:
            observation = "Week werd gevoed door externe bronnen – plan bewust tijd om zelf te reflecteren."
        else:
            observation = "Mooie balans tussen externe input en eigen denkwerk."
        return (
            f"{message_count} berichten verwerkt ({note_count} eigen notities, {external_count} externe bronnen). "
            f"{observation}"
        )

    def _build_themes(
        self,
        platform_counts: Counter,
        format_counts: Counter,
        domain_counts: Counter,
        note_count: int,
        external_count: int,
    ) -> Dict:
        story_chunks: List[str] = []
        total_inputs = note_count + external_count
        short_formats = {"youtube_short", "instagram_reel"}
        long_formats = {"web_article", "generic_webpage", "linkedin_post"}
        short_total = sum(format_counts.get(fmt, 0) for fmt in short_formats)
        long_total = sum(format_counts.get(fmt, 0) for fmt in long_formats)

        if external_count == 0 and note_count > 0:
            story_chunks.append("Je week draaide volledig om eigen notities – perfect moment om een idee live te brengen.")
        elif external_count > 0:
            if format_counts:
                dominant_fmt, dominant_count = format_counts.most_common(1)[0]
                dominance_ratio = dominant_count / max(external_count, 1)
                if dominance_ratio >= 0.5:
                    story_chunks.append(
                        f"Je consumeert vooral {self._format_label(dominant_fmt)} ({dominant_count} items)."
                    )
            if short_total and short_total >= long_total:
                story_chunks.append("Veel short-form input; bouw rust in om ten minste één onderwerp dieper te onderzoeken.")
            elif long_total:
                story_chunks.append("Er zit voldoende long-form input in de mix – destilleer daaruit 1 tot 2 kerninzichten.")

        if note_count == 0 and external_count:
            story_chunks.append("Er ontbreken eigen notities – schrijf na de belangrijkste link kort je eigen take.")
        elif note_count > external_count:
            story_chunks.append("Eigen denkwerk weegt zwaarder dan links; kies een kanaal om dit te delen.")

        platform_story = ""
        if platform_counts:
            top_platform, count = platform_counts.most_common(1)[0]
            platform_story = f"Bundel de {count} inputs vanuit {top_platform} tot één thema en bepaal de volgende stap."
        elif note_count:
            platform_story = "Plan één moment om een eigen notitie te koppelen aan een externe bron."

        domain_note = ""
        if domain_counts:
            top_domain, dom_count = domain_counts.most_common(1)[0]
            domain_note = f" {dom_count} items kwamen van {top_domain}; bepaal waarom dit domein je zo aantrekt."

        story = " ".join(chunk for chunk in story_chunks if chunk).strip()
        if domain_note:
            story = (story + domain_note).strip()

        return {
            "story": story or "Weinig duidelijke thema’s – label je bronnen zodat patronen zichtbaar worden.",
            "platform_story": platform_story,
            "content_formats": format_counts.most_common(5),
            "platforms": platform_counts.most_common(5),
            "domains": domain_counts.most_common(5),
            "short_ratio": (short_total / max(external_count, 1)) if external_count else 0.0,
            "note_count": note_count,
            "external_count": external_count,
        }

    def _links_overview(self, resources: List[Resource]) -> Dict[str, List[Dict]]:
        grouped = {
            "artikelen": [],
            "videos": [],
            "social": [],
            "overig": [],
        }
        for res in resources:
            item = {
                "title": res.title or res.canonical_url or res.url,
                "domain": res.domain or "onbekend",
                "url": res.canonical_url or res.final_url or res.url,
            }
            fmt = (res.content_format or "").lower()
            platform = (res.platform or "").lower()
            if fmt in {"web_article", "generic_webpage", "linkedin_post"}:
                grouped["artikelen"].append(item)
            elif fmt in {"youtube_video", "youtube_short"} or platform == "youtube":
                grouped["videos"].append(item)
            elif platform in {"instagram", "linkedin"}:
                grouped["social"].append(item)
            else:
                grouped["overig"].append(item)
        return grouped

    def _resource_importance(self, res: Resource) -> str:
        format_label = self._format_label(res.content_format)
        domain = f" ({res.domain})" if res.domain else ""
        snippet_source = res.description or res.extracted_text
        snippet = (
            f" – {shorten(snippet_source.strip(), width=70, placeholder='…')}"
            if snippet_source
            else ""
        )
        status = (res.extraction_status or "").lower()
        if status != "success":
            status_note = " (extractie onvolledig)"
        else:
            status_note = ""
        return f"{format_label}{domain}{status_note}{snippet}".strip()

    def _is_strong_resource(self, res: Resource) -> bool:
        status = (res.extraction_status or "").lower()
        if status not in {"success", "partial"}:
            return False
        has_meta = bool(res.title or res.description)
        text_length = len((res.extracted_text or "").strip())
        return has_meta or text_length >= 180

    def _is_noise_resource(self, res: Resource) -> bool:
        url = (res.canonical_url or res.final_url or res.url or "").strip().lower()
        if not url:
            return True
        domain = (res.domain or "").lower()
        title = (res.title or "").strip().lower()
        if "consent.youtube.com" in domain:
            return True
        if url.endswith("/posts/") or url.endswith("/feed/"):
            return True
        if title in {"", "untitled"} and len((res.extracted_text or "").strip()) < 60:
            return True
        return False

    def _resource_dedupe_key(self, res: Resource) -> str:
        raw = (res.canonical_url or res.final_url or res.url or "").strip().lower()
        if not raw:
            return ""
        parsed = urlparse(raw)
        normalized = f"{parsed.netloc}{parsed.path}".rstrip("/")
        return normalized or raw

    def _resource_why_relevant(self, res: Resource) -> str:
        fmt = (res.content_format or "").lower()
        if fmt in {"web_article", "generic_webpage", "linkedin_post"}:
            return "Omdat deze bron inhoudelijk genoeg is om concrete lessen uit te halen."
        if fmt in {"youtube_video", "youtube_short", "instagram_reel", "instagram_post"}:
            return "Omdat dit onderwerp terugkomt in je input en vraagt om gerichte verdieping."
        return "Omdat dit aansluit op je actuele kennisstroom en direct toepasbaar kan zijn."

    def _format_label(self, content_format: Optional[str]) -> str:
        if not content_format:
            return "bron"
        mapping = {
            "web_article": "long-form artikel",
            "generic_webpage": "artikel/post",
            "linkedin_post": "LinkedIn post",
            "instagram_post": "Instagram post",
            "instagram_reel": "Instagram reel",
            "youtube_video": "YouTube-video",
            "youtube_short": "YouTube Short",
            "plain_text": "eigen notitie",
        }
        return mapping.get(content_format, content_format.replace("_", " "))

    def _render_email_body(
        self,
        *,
        subject: str,
        intro: str,
        highlights: List[Dict],
        themes: Dict,
        notes: List[Dict],
        links: Dict[str, List[Dict]],
        actions: List[str],
        reflection: str,
        meta_analysis: str,
    ) -> str:
        """Render a plaintext summary (HTML is added later in EmailService)."""
        lines = [subject, "=" * len(subject), ""]
        lines.append("Intro")
        lines.append("-----")
        lines.append(intro)
        lines.append("")
        if highlights:
            lines.append("Highlights")
            lines.append("---------")
            for hl in highlights:
                if hl.get("type") == "note":
                    lines.append(f"- Eigen idee: {hl['title']} — {hl['detail']}")
                else:
                    lines.append(f"- {hl['title']} — {hl['detail']}")
                    if hl.get("url"):
                        lines.append(f"  {hl['url']}")
            lines.append("")
        if themes.get("story"):
            lines.append("Thema’s")
            lines.append("-------")
            lines.append(themes["story"])
            lines.append("")
        if notes:
            lines.append("Eigen ideeën en notities")
            lines.append("------------------------")
            for note in notes:
                lines.append(f"- {note['text']} ({note['received_at']})")
            lines.append("Deze notities zijn je hefboom; kies minstens één idee om concreet te maken.")
            lines.append("")
        if any(links.values()):
            lines.append("Links en bronnen")
            lines.append("----------------")
            for label, items in links.items():
                if not items:
                    continue
                lines.append(f"{label.capitalize()}:")
                for item in items:
                    lines.append(f"- {item['title']} ({item['domain']})")
                    if item.get("url"):
                        lines.append(f"  {item['url']}")
                lines.append("")
        if actions:
            lines.append("Actiepunten")
            lines.append("-----------")
            for action in actions:
                lines.append(f"- {action}")
            lines.append("")
        lines.append("Reflectie")
        lines.append("---------")
        lines.append(reflection)
        lines.append("")
        lines.append("Meta-analyse")
        lines.append("------------")
        lines.append(meta_analysis)
        lines.append("")
        lines.append("Antwoord gerust met feedback of aanvullingen.")
        return "\n".join(lines)


def _trend_text(delta: int, noun: str) -> str:
    if delta > 0:
        return f"steeg het aantal {noun} met {delta}"
    if delta < 0:
        return f"daalde het aantal {noun} met {abs(delta)}"
    return f"bleef het aantal {noun} gelijk"

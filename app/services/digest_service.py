"""Weekly digest generation service (non-AI)."""
from __future__ import annotations

import logging
import re
from collections import Counter
from datetime import date, datetime, timezone
from hashlib import sha1
from textwrap import shorten
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from app.config import get_settings
from app.db import get_session
from app.models import RawMessage, Resource, WeeklyReport
from app.repositories.weekly_report_repository import WeeklyReportRepository
from app.schemas import AIDigestPayload
from app.services.ai_digest_service import AIDigestService, is_ai_enabled
from app.services.rules_engine import RulesEngine
from app.utils.datetime_utils import format_datetime_for_display


class DigestService:
    """Builds weekly summaries from deterministic extraction data."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.rules_engine = RulesEngine()

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

            report_data = self._build_payload(
                user_id=user_id,
                messages=messages,
                resources=resources,
                previous=previous,
                start=start,
                end=end,
            )
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
        *,
        user_id: int,
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
        analyzed_notes = self._analyze_note_items(ideas)
        note_count = len(ideas)
        external_resources = [
            res for res in resources if not (res.platform == "telegram" and res.content_format == "plain_text")
        ]
        analyzed_resources = self._analyze_resource_items(external_resources)
        platform_counts = Counter((res.platform or "unknown") for res in external_resources)
        format_counts = Counter((res.content_format or "unknown") for res in external_resources)
        domain_counts = Counter((res.domain or "unknown") for res in external_resources)
        topic_counts = Counter(item.get("primary_topic") for item in analyzed_resources + analyzed_notes if item.get("primary_topic"))
        self.rules_engine.apply_topic_recurrence_boost(analyzed_notes, topic_counts)
        self.rules_engine.apply_topic_recurrence_boost(analyzed_resources, topic_counts)
        self._apply_duplicate_score_penalty(analyzed_resources)
        item_type_counts = Counter(item.get("item_type") for item in analyzed_resources + analyzed_notes if item.get("item_type"))
        thinking_count = sum(1 for item in analyzed_resources + analyzed_notes if item.get("is_thinking"))
        consumption_count = sum(1 for item in analyzed_resources + analyzed_notes if item.get("is_consumption"))
        short_form_count = sum(1 for item in analyzed_resources + analyzed_notes if item.get("content_depth") == "short_form")
        long_form_count = sum(1 for item in analyzed_resources + analyzed_notes if item.get("content_depth") == "long_form")

        highlights, duplicates_filtered_count = self._build_highlights(analyzed_resources, analyzed_notes)
        themes = self._build_themes(
            platform_counts,
            format_counts,
            domain_counts,
            topic_counts,
            item_type_counts,
            note_count,
            len(external_resources),
            thinking_count,
            consumption_count,
            short_form_count,
            long_form_count,
        )
        themes["duplicates_filtered_count"] = duplicates_filtered_count
        links_overview = self._links_overview(external_resources)
        actions = self._action_items(analyzed_notes, analyzed_resources, highlights, themes, links_overview)
        reflection = self._build_reflection(message_count, len(external_resources), note_count, themes, links_overview)
        meta_analysis = self._build_meta_analysis(previous, message_count, resource_count)
        intro_summary = self._intro_summary(message_count, len(external_resources), note_count)

        settings = get_settings()
        ai_payload: Optional[AIDigestPayload] = None
        ai_meta: Dict[str, object] = {
            "enabled": False,
            "status": "disabled",
            "reason": "OPENAI_DIGEST_ENABLED is false or OPENAI_API_KEY missing",
            "model": settings.openai_digest_model,
            "call_made": False,
            "tokens_total": 0,
        }
        if is_ai_enabled(settings):
            try:
                ai_result = AIDigestService().enhance(
                    user_id=user_id,
                    start=start,
                    end=end,
                    message_count=message_count,
                    resource_count=resource_count,
                    duplicates_filtered_count=duplicates_filtered_count,
                    notes=ideas,
                    resources=external_resources,
                    highlights=highlights,
                    themes=themes,
                    actions=actions,
                    reflection=reflection,
                    meta_analysis=meta_analysis,
                )
                ai_payload = ai_result.payload
                ai_meta = ai_result.meta
            except Exception as exc:  # pragma: no cover
                self.logger.warning("AI digest enhancement failed hard, fallback to heuristic digest: %s", exc)
                ai_meta = {
                    "enabled": True,
                    "status": "failed_fallback",
                    "reason": str(exc),
                    "model": settings.openai_digest_model,
                    "call_made": True,
                    "tokens_total": 0,
                }
        else:
            self.logger.info("AI digest enhancement skipped: disabled or missing OPENAI_API_KEY")

        themes["ai_enhancement"] = ai_meta
        if ai_payload:
            intro_summary, highlights, themes, ideas, actions, reflection, meta_analysis = self._apply_ai_enhancement(
                ai_payload,
                intro_summary=intro_summary,
                highlights=highlights,
                themes=themes,
                ideas=ideas,
                actions=actions,
                reflection=reflection,
                meta_analysis=meta_analysis,
            )

        subject = f"Weekly Digest {start:%Y-%m-%d} - {end:%Y-%m-%d}"
        body = self._render_email_body(
            subject=subject,
            intro=intro_summary,
            highlights=highlights,
            duplicates_filtered_count=duplicates_filtered_count,
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

    def _apply_ai_enhancement(
        self,
        ai_payload: AIDigestPayload,
        *,
        intro_summary: str,
        highlights: List[Dict],
        themes: Dict,
        ideas: List[Dict],
        actions: List[str],
        reflection: str,
        meta_analysis: str,
    ) -> Tuple[str, List[Dict], Dict, List[Dict], List[str], str, str]:
        intro = ai_payload.summary.strip() or intro_summary
        merged_highlights = self._map_ai_highlights(ai_payload, fallback=highlights)
        if ai_payload.themes:
            themes["story"] = " ".join(ai_payload.themes[:2]).strip()
        if ai_payload.key_insight:
            themes["platform_story"] = ai_payload.key_insight.strip()
        if ai_payload.ideas:
            ideas = [{"received_at": "", "text": item} for item in ai_payload.ideas]
        if ai_payload.actions:
            actions = ai_payload.actions
        reflection = ai_payload.reflection.strip() or reflection
        meta_analysis = ai_payload.meta_analysis.strip() or meta_analysis
        return intro, merged_highlights, themes, ideas, actions, reflection, meta_analysis

    def _map_ai_highlights(self, ai_payload: AIDigestPayload, *, fallback: List[Dict]) -> List[Dict]:
        mapped: List[Dict] = []
        for highlight in ai_payload.highlights[:5]:
            title = highlight.title.strip()
            if not title:
                continue
            mapped.append(
                {
                    "type": "resource",
                    "title": title,
                    "detail": "AI highlight",
                    "why_relevant": highlight.why_relevant.strip(),
                }
            )
        return mapped or fallback

    def _build_highlights(self, resources: List[Dict[str, Any]], ideas: List[Dict[str, Any]]) -> Tuple[List[Dict], int]:
        highlights: List[Dict] = []
        duplicates_filtered_count = 0
        seen_note_keys = set()
        target_max = 5
        # 1) Prioritize high-value own notes first.
        sorted_ideas = sorted(
            ideas,
            key=lambda n: (n.get("importance_score", 0), len(n["text"]), n["received_at"]),
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
                    "detail": f"{note.get('item_type', 'note')} · topic={note.get('primary_topic', 'overig')} · score={note.get('importance_score', 0)}/10",
                    "why_relevant": "Eigen denkwerk met hoge impact op je volgende acties.",
                    "importance_score": note.get("importance_score", 0),
                    "primary_topic": note.get("primary_topic", "knowledge_management"),
                    "item_type": note.get("item_type", "note"),
                }
            )
            if len(highlights) >= 2:
                break

        # 2) Add strong external resources while filtering noise and duplicates.
        seen_resource_keys = set()
        seen_titles_by_base_key: Dict[str, set[str]] = {}
        candidates = [
            item
            for item in resources
            if self._is_strong_resource(item["resource"]) and not self._is_noise_resource(item["resource"])
        ]
        candidates.sort(
            key=lambda item: (
                item.get("importance_score", 0),
                (item["resource"].fetched_at or datetime.min),
                len((item["resource"].description or item["resource"].extracted_text or "")[:240]),
            ),
            reverse=True,
        )
        for item in candidates:
            res = item["resource"]
            base_key = self._resource_dedupe_key(res)
            dedupe_key = base_key
            if not dedupe_key:
                duplicates_filtered_count += 1
                continue

            title_signature = self._title_signature(res.title)
            known_title_signatures = seen_titles_by_base_key.get(base_key, set())
            if dedupe_key in seen_resource_keys:
                # Keep items apart when a URL key collides but titles clearly describe different content.
                if title_signature and known_title_signatures and title_signature not in known_title_signatures:
                    dedupe_key = f"{base_key}::title:{sha1(title_signature.encode('utf-8')).hexdigest()[:10]}"
                else:
                    duplicates_filtered_count += 1
                    continue

            seen_resource_keys.add(dedupe_key)
            if title_signature:
                seen_titles_by_base_key.setdefault(base_key, set()).add(title_signature)
            detail_text = shorten(self._resource_importance(res), width=110, placeholder="…")
            highlights.append(
                {
                    "type": "resource",
                    "title": res.title or res.canonical_url or res.url,
                    "detail": f"{detail_text} · {item.get('item_type', 'resource')} · topic={item.get('primary_topic', 'media')} · score={item.get('importance_score', 0)}/10",
                    "url": res.canonical_url or res.final_url or res.url,
                    "why_relevant": self._resource_why_relevant(res, signal=item),
                    "importance_score": item.get("importance_score", 0),
                    "primary_topic": item.get("primary_topic", "media"),
                    "item_type": item.get("item_type", "resource"),
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
                    "detail": f"{note.get('item_type', 'note')} · topic={note.get('primary_topic', 'overig')} · score={note.get('importance_score', 0)}/10",
                    "why_relevant": "Eigen denkwerk met hoge impact op je volgende acties.",
                    "importance_score": note.get("importance_score", 0),
                    "primary_topic": note.get("primary_topic", "knowledge_management"),
                    "item_type": note.get("item_type", "note"),
                },
            )
            highlights = highlights[:target_max]
        return highlights[:target_max], duplicates_filtered_count

    def _plain_text_notes(self, messages: List[RawMessage]) -> List[Dict]:
        notes = []
        for msg in messages:
            if msg.contains_link:
                continue
            notes.append(
                {
                    "received_at": format_datetime_for_display(msg.received_at),
                    "text": msg.text.strip(),
                }
            )
        return notes[:10]

    def _analyze_note_items(self, notes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        analyzed: List[Dict[str, Any]] = []
        for note in notes:
            signals = self.rules_engine.analyze_note(note.get("text", ""))
            enriched = dict(note)
            enriched.update(signals.as_dict())
            analyzed.append(enriched)
        return analyzed

    def _analyze_resource_items(self, resources: List[Resource]) -> List[Dict[str, Any]]:
        analyzed: List[Dict[str, Any]] = []
        for resource in resources:
            related_text = ""
            raw_message = getattr(resource, "raw_message", None)
            if raw_message and raw_message.text:
                related_text = raw_message.text
            signals = self.rules_engine.analyze_resource(resource, related_text=related_text)
            analyzed.append({"resource": resource, **signals.as_dict()})
        return analyzed

    def _apply_duplicate_score_penalty(self, analyzed_resources: List[Dict[str, Any]]) -> None:
        key_counts = Counter(
            self._resource_dedupe_key(item["resource"])
            for item in analyzed_resources
            if self._resource_dedupe_key(item["resource"])
        )
        seen_per_key: Counter[str] = Counter()
        for item in analyzed_resources:
            key = self._resource_dedupe_key(item["resource"])
            if not key:
                continue
            seen_per_key[key] += 1
            if key_counts[key] > 1 and seen_per_key[key] > 1:
                item["importance_score"] = max(0, int(item.get("importance_score", 0)) - 2)

    def _action_items(
        self,
        notes: List[Dict[str, Any]],
        analyzed_resources: List[Dict[str, Any]],
        highlights: List[Dict],
        themes: Dict,
        links: Dict[str, List[Dict]],
    ) -> List[str]:
        actions: List[str] = []

        if notes:
            top_notes = sorted(notes, key=lambda note: note.get("importance_score", 0), reverse=True)
            for note in top_notes[:1]:
                snippet = shorten(note["text"], width=70, placeholder="…")
                actions.append(
                    f"Werk dit {note.get('item_type', 'idee')} uit in 5 bullets: '{snippet}' (topic: {note.get('primary_topic', 'overig')})."
                )
        else:
            actions.append("Schrijf vandaag 1 eigen notitie in 5 bullets op basis van je belangrijkste bron.")

        resource_highlights = sorted(
            [hl for hl in highlights if hl.get("type") == "resource"],
            key=lambda item: item.get("importance_score", 0),
            reverse=True,
        )
        for hl in resource_highlights[:2]:
            topic = hl.get("primary_topic", "thema")
            actions.append(f"Lees '{hl['title']}' gericht en noteer 3 inzichten over {topic} die je deze week toepast.")
            if len(actions) >= 3:
                break

        short_ratio = themes.get("short_ratio", 0.0)
        if len(actions) < 3 and links.get("videos") and short_ratio >= 0.5:
            actions.append("Kies 1 korte video en vervang die vandaag door 1 long-form artikel over hetzelfde onderwerp.")

        if len(actions) < 3:
            top_resource_tasks = [
                item for item in analyzed_resources if item.get("item_type") in {"task", "question", "idea"}
            ]
            if top_resource_tasks:
                best = sorted(top_resource_tasks, key=lambda item: item.get("importance_score", 0), reverse=True)[0]
                topic = best.get("primary_topic", "thema")
                actions.append(f"Pak je hoogste-prioriteit resource in '{topic}' en vertaal die naar 1 concrete volgende stap.")

        if len(actions) < 3 and themes.get("thinking_count", 0) < themes.get("consumption_count", 0):
            actions.append("Plan vanavond 15 minuten om je belangrijkste bron om te zetten in één eigen notitie met vervolgstap.")

        if len(actions) < 3:
            top_topic = (themes.get("top_topics") or ["je hoofdthema"])[0]
            actions.append(f"Kies 1 highlight binnen '{top_topic}' en vat die samen in 3 concrete lessen voor morgen.")

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

        thinking_count = int(themes.get("thinking_count", 0) or 0)
        consumption_count = int(themes.get("consumption_count", 0) or 0)
        total_inputs = max(thinking_count + consumption_count, notes + external_resources)
        consumption_ratio = (consumption_count / total_inputs) if total_inputs else 0
        if consumption_ratio >= 0.7:
            focus = "Je week was consumptiegedreven; veel externe input maar weinig eigen verwerking."
            lever = "pak één bron en schrijf er een eigen take bij."
        elif consumption_ratio <= 0.3:
            focus = "Je week draaide vooral om eigen denkwerk – sterk, mits je nu durft te publiceren."
            lever = "kies één idee en maak het zichtbaar."
        else:
            focus = "Er was een gezonde mix tussen externe bronnen en eigen notities."
            lever = "zorg dat minstens één link of idee eindigt in een concrete actie."

        short_ratio = float(themes.get("short_ratio", 0.0) or 0.0)
        if short_ratio >= 0.6:
            format_comment = "Veel short-form content; vertraag bewust zodat inzichten blijven hangen."
        elif themes.get("content_formats"):
            format_comment = "Je mix bevat voldoende long-form materiaal om diepte te pakken."
        else:
            format_comment = "Weinig gelabelde formats – log meer context zodat je patronen ziet."

        top_topic = (themes.get("top_topics") or ["overig"])[0]
        if short_ratio >= 0.5 and notes == 0:
            observation = "Duidelijke observatie: je consumeert veel korte content maar schrijft weinig."
        elif thinking_count > consumption_count:
            observation = f"Duidelijke observatie: je verwerkt actief in eigen woorden, vooral rond '{top_topic}'."
        else:
            observation = f"Duidelijke observatie: je input is vooral consumptie, met beperkte eigen uitwerking rond '{top_topic}'."

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
        topic_counts: Counter,
        item_type_counts: Counter,
        note_count: int,
        external_count: int,
        thinking_count: int,
        consumption_count: int,
        short_form_count: int,
        long_form_count: int,
    ) -> Dict:
        story_chunks: List[str] = []
        short_formats = {"youtube_short", "instagram_reel"}
        long_formats = {"web_article", "generic_webpage", "linkedin_post"}
        short_total = sum(format_counts.get(fmt, 0) for fmt in short_formats)
        long_total = sum(format_counts.get(fmt, 0) for fmt in long_formats)
        short_total = max(short_total, short_form_count)
        long_total = max(long_total, long_form_count)

        if topic_counts:
            top_topic, topic_count = topic_counts.most_common(1)[0]
            story_chunks.append(f"Dominant onderwerp: {top_topic} ({topic_count} signalen).")
        if item_type_counts:
            top_type, type_count = item_type_counts.most_common(1)[0]
            story_chunks.append(f"Meest voorkomende intentie: {top_type} ({type_count} items).")

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
        if thinking_count and consumption_count:
            if thinking_count > consumption_count:
                story_chunks.append("Je thinking-signalen zijn sterker dan consumptie; dit is een goed moment om output te publiceren.")
            elif consumption_count > thinking_count:
                story_chunks.append("Consumptie domineert thinking; plan bewust een vertaalslag naar eigen notities.")

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
            "top_topics": [topic for topic, _ in topic_counts.most_common(5)],
            "item_types": item_type_counts.most_common(5),
            "thinking_count": thinking_count,
            "consumption_count": consumption_count,
            "short_form_count": short_total,
            "long_form_count": long_total,
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
        raw_url = (res.canonical_url or res.final_url or res.url or "").strip()
        if not raw_url:
            return ""
        parsed = urlparse(raw_url)
        if not parsed.scheme or not parsed.netloc:
            return raw_url.lower()

        domain = parsed.netloc.lower().replace("www.", "")
        path_full = parsed.path or "/"
        path_lower = path_full.lower()
        is_linkedin_post = domain.endswith("linkedin.com") and (
            "linkedin.com/posts/" in raw_url.lower()
            or "linkedin.com/feed/update/" in raw_url.lower()
            or "/pulse/" in path_lower
        )

        filtered_qs = [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if not (
                key.lower().startswith("utm_")
                or key.lower() in {"igsh", "si", "feature", "fbclid", "gclid", "mc_cid", "mc_eid", "trk", "trackingid"}
            )
        ]
        filtered_qs.sort()
        normalized_query = urlencode(filtered_qs, doseq=True)

        normalized = parsed._replace(
            netloc=domain,
            path=(path_full.rstrip("/") or "/"),
            query=normalized_query,
            fragment="",
        )
        if is_linkedin_post:
            # Keep full LinkedIn post path/slug so separate posts never collapse.
            linkedin_key = f"linkedin_post::{domain}{path_full}"
            if normalized_query:
                linkedin_key = f"{linkedin_key}?{normalized_query}"
            return linkedin_key
        return urlunparse(normalized)

    def _title_signature(self, title: Optional[str]) -> str:
        value = (title or "").strip().lower()
        if not value:
            return ""
        normalized = re.sub(r"\s+", " ", value)
        normalized = re.sub(r"[^a-z0-9 ]+", "", normalized)
        return normalized[:120]

    def _resource_why_relevant(self, res: Resource, *, signal: Optional[Dict[str, Any]] = None) -> str:
        fmt = (res.content_format or "").lower()
        topic = (signal or {}).get("primary_topic", "")
        score = (signal or {}).get("importance_score")
        if fmt in {"web_article", "generic_webpage", "linkedin_post"}:
            base = "Omdat deze bron inhoudelijk genoeg is om concrete lessen uit te halen."
        elif fmt in {"youtube_video", "youtube_short", "instagram_reel", "instagram_post"}:
            base = "Omdat dit onderwerp terugkomt in je input en vraagt om gerichte verdieping."
        else:
            base = "Omdat dit aansluit op je actuele kennisstroom en direct toepasbaar kan zijn."
        suffix = []
        if topic:
            suffix.append(f"topic={topic}")
        if score is not None:
            suffix.append(f"score={score}/10")
        if suffix:
            return f"{base} ({', '.join(suffix)})"
        return base

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
        duplicates_filtered_count: int,
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
        lines.append(f"Voor dit overzicht zijn {duplicates_filtered_count} dubbele items verwijderd.")
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
                if hl.get("why_relevant"):
                    lines.append(f"  Waarom dit relevant is: {hl['why_relevant']}")
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

"""Weekly/daily digest generation service (deterministic, non-AI)."""
from __future__ import annotations

import logging
import re
from collections import Counter
from datetime import date, datetime, timezone
from hashlib import sha1
from textwrap import shorten
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from app.db import get_session
from app.models import RawMessage, Resource, WeeklyReport
from app.repositories.weekly_report_repository import WeeklyReportRepository
from app.services.rules_engine import RulesEngine
from app.utils.datetime_utils import format_datetime_for_display


class DigestService:
    """Build deterministic digest reports from messages + extracted resources."""

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
        """Generate and store one report for the given user/period."""
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
                "Generated digest report %s for user %s (%s - %s)",
                report.id,
                user_id,
                start,
                end,
            )
            return report

    def _load_messages(
        self,
        session,
        user_id: int,
        start: date,
        end: date,
    ) -> List[RawMessage]:
        start_dt = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
        end_dt = datetime.combine(end, datetime.max.time(), tzinfo=timezone.utc)
        return (
            session.query(RawMessage)
            .filter(
                RawMessage.user_id == user_id,
                RawMessage.received_at.between(start_dt, end_dt),
            )
            .order_by(RawMessage.received_at.asc(), RawMessage.id.asc())
            .all()
        )

    def _load_resources(
        self,
        session,
        user_id: int,
        start: date,
        end: date,
    ) -> List[Resource]:
        start_dt = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
        end_dt = datetime.combine(end, datetime.max.time(), tzinfo=timezone.utc)
        return (
            session.query(Resource)
            .join(RawMessage)
            .filter(
                RawMessage.user_id == user_id,
                RawMessage.received_at.between(start_dt, end_dt),
            )
            .order_by(Resource.id.asc())
            .all()
        )

    def _build_payload(
        self,
        *,
        messages: List[RawMessage],
        resources: List[Resource],
        previous: Optional[WeeklyReport],
        start: date,
        end: date,
    ) -> Dict[str, Any]:
        message_count = len(messages)
        resource_count = len(resources)

        notes = self._plain_text_notes(messages)
        analyzed_notes = self._analyze_note_items(notes)

        external_resources = [
            resource
            for resource in resources
            if not (resource.platform == "telegram" and resource.content_format == "plain_text")
        ]
        analyzed_resources = self._analyze_resource_items(external_resources)

        platform_counts = Counter((resource.platform or "unknown") for resource in external_resources)
        format_counts = Counter((resource.content_format or "unknown") for resource in external_resources)
        domain_counts = Counter((resource.domain or "unknown") for resource in external_resources)
        topic_counts = Counter(
            item.get("primary_topic")
            for item in analyzed_notes + analyzed_resources
            if item.get("primary_topic")
        )

        self.rules_engine.apply_topic_recurrence_boost(analyzed_notes, topic_counts)
        self.rules_engine.apply_topic_recurrence_boost(analyzed_resources, topic_counts)
        self._apply_duplicate_score_penalty(analyzed_resources)

        item_type_counts = Counter(
            item.get("item_type")
            for item in analyzed_notes + analyzed_resources
            if item.get("item_type")
        )
        behavior = self._build_behavior_signals(analyzed_notes, analyzed_resources)

        highlights, duplicates_filtered_count = self._select_highlights_with_reasons(
            analyzed_resources,
            analyzed_notes,
        )
        topic_summary = self._build_topic_summary(topic_counts)
        item_type_summary = self._build_item_type_summary(item_type_counts)

        themes = self._build_topic_theme_summary(
            platform_counts=platform_counts,
            format_counts=format_counts,
            domain_counts=domain_counts,
            topic_summary=topic_summary,
            item_type_summary=item_type_summary,
            behavior=behavior,
            note_count=len(analyzed_notes),
            external_count=len(external_resources),
        )
        themes["duplicates_filtered_count"] = duplicates_filtered_count
        themes["average_importance_score"] = behavior["average_importance_score"]
        themes["high_importance_count"] = behavior["high_importance_count"]
        themes["topic_diversity"] = len(topic_counts)

        actions = self._build_action_items(
            notes=analyzed_notes,
            highlights=highlights,
            behavior=behavior,
            topic_summary=topic_summary,
        )
        reflection = self._build_reflection(
            message_count=message_count,
            note_count=len(analyzed_notes),
            external_count=len(external_resources),
            behavior=behavior,
            topic_summary=topic_summary,
            item_type_summary=item_type_summary,
        )
        meta_analysis = self._build_meta_analysis(previous, message_count, resource_count)
        intro_summary = self._build_executive_summary(
            message_count=message_count,
            note_count=len(analyzed_notes),
            external_count=len(external_resources),
            duplicates_filtered_count=duplicates_filtered_count,
            behavior=behavior,
        )
        key_insight = self._build_key_insight(
            behavior=behavior,
            topic_summary=topic_summary,
            note_count=len(analyzed_notes),
            external_count=len(external_resources),
        )

        all_messages = self._build_full_notes_log(messages)
        all_resources = self._build_full_resources_log(analyzed_resources)
        themes["full_overview"] = {
            "messages": all_messages,
            "resources": all_resources,
        }

        subject = f"Weekly Digest {start:%Y-%m-%d} - {end:%Y-%m-%d}"
        email_body = self._render_email_body(
            subject=subject,
            intro_summary=intro_summary,
            key_insight=key_insight,
            highlights=highlights,
            themes=themes,
            actions=actions,
            reflection=reflection,
            meta_analysis=meta_analysis,
            all_messages=all_messages,
            all_resources=all_resources,
        )

        return {
            "source_message_count": message_count,
            "source_resource_count": resource_count,
            "highlights_json": highlights,
            "themes_json": themes,
            "ideas_json": notes,
            "actions_json": actions,
            "reflection": reflection,
            "meta_analysis": meta_analysis,
            "email_subject": subject,
            "email_body": email_body,
            "status": "generated",
            "generated_at": datetime.now(timezone.utc),
        }

    def _plain_text_notes(self, messages: Sequence[RawMessage]) -> List[Dict[str, Any]]:
        notes: List[Dict[str, Any]] = []
        for message in messages:
            if message.contains_link:
                continue
            text = (message.text or "").strip()
            if not text:
                continue
            notes.append(
                {
                    "received_at": format_datetime_for_display(message.received_at),
                    "text": text,
                }
            )
        return notes

    def _analyze_note_items(self, notes: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        analyzed: List[Dict[str, Any]] = []
        for note in notes:
            signals = self.rules_engine.analyze_note(note.get("text", ""))
            enriched = dict(note)
            enriched.update(signals.as_dict())
            analyzed.append(enriched)
        return analyzed

    def _analyze_resource_items(self, resources: Sequence[Resource]) -> List[Dict[str, Any]]:
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
        keys = [self._resource_dedupe_key(item["resource"]) for item in analyzed_resources]
        counts = Counter(key for key in keys if key)
        seen: Counter[str] = Counter()
        for item in analyzed_resources:
            key = self._resource_dedupe_key(item["resource"])
            if not key:
                continue
            seen[key] += 1
            if counts[key] > 1 and seen[key] > 1:
                item["importance_score"] = max(0, int(item.get("importance_score", 0)) - 2)

    def _build_behavior_signals(
        self,
        analyzed_notes: Sequence[Dict[str, Any]],
        analyzed_resources: Sequence[Dict[str, Any]],
    ) -> Dict[str, Any]:
        all_items = list(analyzed_notes) + list(analyzed_resources)
        thinking_count = sum(1 for item in all_items if item.get("is_thinking"))
        consumption_count = sum(1 for item in all_items if item.get("is_consumption"))
        short_form_count = sum(1 for item in all_items if item.get("content_depth") == "short_form")
        long_form_count = sum(1 for item in all_items if item.get("content_depth") == "long_form")
        scores = [int(item.get("importance_score", 0)) for item in all_items]
        total = len(all_items)
        return {
            "thinking_count": thinking_count,
            "consumption_count": consumption_count,
            "short_form_count": short_form_count,
            "long_form_count": long_form_count,
            "average_importance_score": (sum(scores) / len(scores)) if scores else 0.0,
            "high_importance_count": sum(1 for score in scores if score >= 7),
            "thinking_ratio": (thinking_count / total) if total else 0.0,
            "consumption_ratio": (consumption_count / total) if total else 0.0,
            "short_ratio": (short_form_count / total) if total else 0.0,
            "long_ratio": (long_form_count / total) if total else 0.0,
        }

    def _build_topic_summary(self, topic_counts: Counter) -> List[Tuple[str, int]]:
        return topic_counts.most_common(5)

    def _build_item_type_summary(self, item_type_counts: Counter) -> List[Tuple[str, int]]:
        return item_type_counts.most_common(5)

    def _select_highlights_with_reasons(
        self,
        analyzed_resources: Sequence[Dict[str, Any]],
        analyzed_notes: Sequence[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], int]:
        highlights: List[Dict[str, Any]] = []
        duplicates_filtered_count = 0

        seen_note_keys: set[str] = set()
        for note in sorted(
            analyzed_notes,
            key=lambda item: (int(item.get("importance_score", 0)), len(item.get("text", ""))),
            reverse=True,
        ):
            key = (note.get("text") or "").strip().lower()
            if not key or key in seen_note_keys:
                continue
            seen_note_keys.add(key)
            highlights.append(
                {
                    "type": "note",
                    "label": "Eigen idee",
                    "title": shorten(note.get("text", ""), width=95, placeholder="…"),
                    "detail": self._build_highlight_detail(
                        topic=note.get("primary_topic"),
                        item_type=note.get("item_type"),
                    ),
                    "why_relevant": self._build_note_why_relevant(note),
                    "primary_topic": note.get("primary_topic", "knowledge_management"),
                    "item_type": note.get("item_type", "note"),
                    "importance_score": int(note.get("importance_score", 0)),
                }
            )
            if len([item for item in highlights if item["type"] == "note"]) >= 2:
                break

        seen_resource_keys: set[str] = set()
        seen_title_signatures: Dict[str, set[str]] = {}
        candidates = [
            item
            for item in analyzed_resources
            if self._is_strong_resource(item["resource"]) and not self._is_noise_resource(item["resource"])
        ]
        candidates.sort(
            key=lambda item: (
                int(item.get("importance_score", 0)),
                item["resource"].fetched_at or datetime.min,
            ),
            reverse=True,
        )
        for item in candidates:
            resource = item["resource"]
            base_key = self._resource_dedupe_key(resource)
            dedupe_key = base_key
            if not base_key:
                duplicates_filtered_count += 1
                continue

            title_signature = self._title_signature(resource.title)
            known_signatures = seen_title_signatures.get(base_key, set())
            if dedupe_key in seen_resource_keys:
                if title_signature and known_signatures and title_signature not in known_signatures:
                    dedupe_key = f"{base_key}::title:{sha1(title_signature.encode('utf-8')).hexdigest()[:10]}"
                else:
                    duplicates_filtered_count += 1
                    continue

            seen_resource_keys.add(dedupe_key)
            if title_signature:
                seen_title_signatures.setdefault(base_key, set()).add(title_signature)

            highlights.append(
                {
                    "type": "resource",
                    "title": resource.title or resource.canonical_url or resource.url,
                    "detail": self._build_highlight_detail(
                        topic=item.get("primary_topic"),
                        item_type=item.get("item_type"),
                    ),
                    "why_relevant": self._resource_why_relevant(resource, signal=item),
                    "url": resource.canonical_url or resource.final_url or resource.url,
                    "primary_topic": item.get("primary_topic", "media"),
                    "item_type": item.get("item_type", "resource"),
                    "importance_score": int(item.get("importance_score", 0)),
                }
            )

            if len(highlights) >= 5:
                break

        if analyzed_notes and not any(item.get("type") == "note" for item in highlights):
            note = analyzed_notes[0]
            highlights.insert(
                0,
                {
                    "type": "note",
                    "label": "Eigen idee",
                    "title": shorten(note.get("text", ""), width=95, placeholder="…"),
                    "detail": self._build_highlight_detail(
                        topic=note.get("primary_topic"),
                        item_type=note.get("item_type"),
                    ),
                    "why_relevant": self._build_note_why_relevant(note),
                    "primary_topic": note.get("primary_topic", "knowledge_management"),
                    "item_type": note.get("item_type", "note"),
                    "importance_score": int(note.get("importance_score", 0)),
                },
            )

        highlights.sort(key=lambda item: int(item.get("importance_score", 0)), reverse=True)
        note_highlights = [item for item in highlights if item.get("type") == "note"]
        resource_highlights = [item for item in highlights if item.get("type") == "resource"]
        mixed = note_highlights[:2] + resource_highlights[:3]
        for item in highlights:
            if len(mixed) >= 5:
                break
            if item not in mixed:
                mixed.append(item)

        return mixed[:5], duplicates_filtered_count

    def _build_highlight_detail(self, *, topic: Optional[str], item_type: Optional[str]) -> str:
        topic_value = topic or "overig"
        item_type_value = item_type or "other"
        return f"topic={topic_value} · type={item_type_value}"

    def _build_note_why_relevant(self, note: Dict[str, Any]) -> str:
        score = int(note.get("importance_score", 0))
        item_type = note.get("item_type", "note")
        topic = note.get("primary_topic", "overig")
        if score >= 8:
            return f"Sterke eigen input ({item_type}) binnen '{topic}' met directe kans op actie deze week."
        if score >= 6:
            return f"Deze eigen notitie is een bruikbare bouwsteen voor je thema '{topic}'."
        return f"Eigen notities maken je denkwerk zichtbaar; gebruik dit als startpunt binnen '{topic}'."

    def _build_topic_theme_summary(
        self,
        *,
        platform_counts: Counter,
        format_counts: Counter,
        domain_counts: Counter,
        topic_summary: List[Tuple[str, int]],
        item_type_summary: List[Tuple[str, int]],
        behavior: Dict[str, Any],
        note_count: int,
        external_count: int,
    ) -> Dict[str, Any]:
        patterns: List[str] = []

        if topic_summary:
            topic, count = topic_summary[0]
            patterns.append(f"Toponderwerp: {topic} ({count} signalen).")
        if item_type_summary:
            item_type, count = item_type_summary[0]
            patterns.append(f"Dominante intentie: {item_type} ({count} items).")

        thinking_count = int(behavior["thinking_count"])
        consumption_count = int(behavior["consumption_count"])
        if thinking_count > consumption_count:
            patterns.append("Je week is thinking-gedreven: meer verwerking dan consumptie.")
        elif consumption_count > thinking_count:
            patterns.append("Je week is consumptie-gedreven: plan expliciet verwerking in eigen woorden.")

        short_count = int(behavior["short_form_count"])
        long_count = int(behavior["long_form_count"])
        if short_count > long_count:
            patterns.append("Short-form input overheerst; reserveer tijd voor verdieping.")
        elif long_count > short_count and long_count > 0:
            patterns.append("Long-form input is sterk aanwezig; vertaal dit naar concrete output.")

        if note_count == 0 and external_count > 0:
            patterns.append("Er zijn geen eigen notities; voeg na elke kernbron minimaal 3 zinnen eigen duiding toe.")
        elif note_count > external_count:
            patterns.append("Eigen notities wegen zwaarder dan externe links; dit is een sterk signaal van actief denken.")

        story = " ".join(patterns[:3]).strip() or "Weinig duidelijke thema’s – voeg meer context toe om patronen te zien."

        return {
            "story": story,
            "patterns": patterns[:5],
            "platform_story": self._build_platform_story(platform_counts, note_count),
            "content_formats": format_counts.most_common(5),
            "platforms": platform_counts.most_common(5),
            "domains": domain_counts.most_common(5),
            "top_topics": [topic for topic, _ in topic_summary],
            "item_types": item_type_summary,
            "thinking_count": thinking_count,
            "consumption_count": consumption_count,
            "short_form_count": short_count,
            "long_form_count": long_count,
            "short_ratio": float(behavior["short_ratio"]),
            "note_count": note_count,
            "external_count": external_count,
        }

    def _build_platform_story(self, platform_counts: Counter, note_count: int) -> str:
        if platform_counts:
            platform, count = platform_counts.most_common(1)[0]
            return f"Grootste bronplatform: {platform} ({count} items)."
        if note_count:
            return "Alle input komt uit eigen notities; koppel minstens één notitie aan een externe bron."
        return "Nog geen platformsignalen beschikbaar."

    def _build_action_items(
        self,
        *,
        notes: Sequence[Dict[str, Any]],
        highlights: Sequence[Dict[str, Any]],
        behavior: Dict[str, Any],
        topic_summary: Sequence[Tuple[str, int]],
    ) -> List[str]:
        actions: List[str] = []

        if notes:
            top_note = sorted(notes, key=lambda item: int(item.get("importance_score", 0)), reverse=True)[0]
            snippet = shorten(top_note.get("text", ""), width=70, placeholder="…")
            actions.append(
                f"Werk dit idee uit in 5 bullets: '{snippet}' (probleem, aanpak, risico, eerste stap, deadline)."
            )

        top_resource_highlights = [item for item in highlights if item.get("type") == "resource"]
        if top_resource_highlights:
            first = top_resource_highlights[0]
            actions.append(f"Lees '{first.get('title', 'deze bron')}' opnieuw en noteer direct 3 toepasbare inzichten.")

        if behavior["consumption_count"] > behavior["thinking_count"]:
            actions.append("Plan vandaag 15 minuten om 1 geconsumeerde bron om te zetten naar een eigen notitie met vervolgstap.")
        elif behavior["short_form_count"] > behavior["long_form_count"]:
            actions.append("Vervang morgen 1 short-form item door 1 long-form bron binnen hetzelfde onderwerp.")

        if len(actions) < 3 and topic_summary:
            topic = topic_summary[0][0]
            actions.append(f"Kies één item uit '{topic}' en zet het binnen 24 uur om in een concrete actie in je agenda.")

        if len(actions) < 3:
            actions.append("Sluit je dag af met een mini-review: wat behouden, stoppen, starten en verdiepen.")

        deduped: List[str] = []
        seen: set[str] = set()
        for action in actions:
            key = action.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(action)
            if len(deduped) >= 3:
                break
        return deduped

    def _build_reflection(
        self,
        *,
        message_count: int,
        note_count: int,
        external_count: int,
        behavior: Dict[str, Any],
        topic_summary: Sequence[Tuple[str, int]],
        item_type_summary: Sequence[Tuple[str, int]],
    ) -> str:
        lines = self._build_reflection_sections(
            message_count=message_count,
            note_count=note_count,
            external_count=external_count,
            behavior=behavior,
            topic_summary=topic_summary,
            item_type_summary=item_type_summary,
        )
        return "\n".join(lines)

    def _build_reflection_sections(
        self,
        *,
        message_count: int,
        note_count: int,
        external_count: int,
        behavior: Dict[str, Any],
        topic_summary: Sequence[Tuple[str, int]],
        item_type_summary: Sequence[Tuple[str, int]],
    ) -> List[str]:
        if message_count == 0 and external_count == 0:
            return [
                "Geen activiteit in deze periode.",
                "Plan een vast capture-moment en leg minimaal één eigen notitie vast.",
            ]

        top_topic = topic_summary[0][0] if topic_summary else "overig"
        top_item_type = item_type_summary[0][0] if item_type_summary else "other"

        thinking_count = int(behavior["thinking_count"])
        consumption_count = int(behavior["consumption_count"])
        short_count = int(behavior["short_form_count"])
        long_count = int(behavior["long_form_count"])
        avg_score = float(behavior["average_importance_score"])
        high_count = int(behavior["high_importance_count"])

        lines: List[str] = []
        lines.append(
            f"Focus lag vooral op '{top_topic}' met '{top_item_type}' als dominante intentie."
        )
        if consumption_count > thinking_count:
            lines.append("Je consumeert meer dan je verwerkt; zonder vertaalslag gaat veel waarde verloren.")
        elif thinking_count > consumption_count:
            lines.append("Je verwerkt actief in eigen woorden; dit vergroot de kans op echte voortgang.")
        else:
            lines.append("Consumptie en denkwerk waren in balans; behoud dit ritme met vaste reviewmomenten.")

        if short_count > long_count:
            lines.append("Short-form input overheerst; kies bewust voor meer diepgang op je hoofdthema.")
        elif long_count > short_count:
            lines.append("Long-form input is sterk; haal hier expliciet concrete acties uit om momentum te houden.")

        lines.append(
            f"Gemiddelde belangrijkheid: {avg_score:.1f}/10 met {high_count} items met hoge prioriteit."
        )

        if note_count == 0 and external_count > 0:
            lines.append("Hefboom: voeg dagelijks minimaal één eigen notitie toe zodat inzichten blijven hangen.")
        elif note_count > 0:
            lines.append("Hefboom: werk deze week één van je eigen notities volledig uit tot een tastbare output.")

        return lines[:6]

    def _build_meta_analysis(
        self,
        previous: Optional[WeeklyReport],
        current_messages: int,
        current_resources: int,
    ) -> str:
        if not previous:
            return "Eerste rapport voor deze gebruiker; vergelijking met vorige periode volgt vanaf de volgende run."
        delta_messages = current_messages - int(previous.source_message_count or 0)
        delta_resources = current_resources - int(previous.source_resource_count or 0)
        return (
            f"Ten opzichte van vorige periode {_trend_text(delta_messages, 'berichten')} "
            f"en {_trend_text(delta_resources, 'bronnen')}."
        )

    def _build_executive_summary(
        self,
        *,
        message_count: int,
        note_count: int,
        external_count: int,
        duplicates_filtered_count: int,
        behavior: Dict[str, Any],
    ) -> str:
        if message_count == 0 and external_count == 0:
            return "Geen input in deze periode. Plan een kort captureblok en leg minstens één notitie en één link vast."

        balance = (
            f"thinking {behavior['thinking_count']} vs consumptie {behavior['consumption_count']}"
            if (behavior["thinking_count"] or behavior["consumption_count"])
            else "nog geen duidelijke gedragsverdeling"
        )
        return (
            f"{message_count} berichten, {external_count} externe bronnen en {note_count} eigen notities verwerkt. "
            f"{duplicates_filtered_count} duplicaten gefilterd. "
            f"Gedragsprofiel: {balance}."
        )

    def _build_key_insight(
        self,
        *,
        behavior: Dict[str, Any],
        topic_summary: Sequence[Tuple[str, int]],
        note_count: int,
        external_count: int,
    ) -> str:
        top_topic = topic_summary[0][0] if topic_summary else "je hoofdthema"
        if note_count == 0 and external_count > 0:
            return f"Je grootste winst zit in eigen notities rond '{top_topic}' in plaats van alleen nieuwe links verzamelen."
        if behavior["consumption_count"] > behavior["thinking_count"]:
            return f"Je grootste hefboom zit in vertaling van consumptie naar actie binnen '{top_topic}'."
        if behavior["thinking_count"] > behavior["consumption_count"]:
            return f"Je denkt al actief mee rond '{top_topic}'; zet nu één idee om in concrete output."
        return f"Behoud je balans in '{top_topic}' en koppel elk sterk item aan een vervolgstap."

    def _build_full_notes_log(self, messages: Sequence[RawMessage]) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        for message in messages:
            text = (message.text or "").strip()
            signals = self.rules_engine.analyze_note(text, has_link=bool(message.contains_link)).as_dict()
            entries.append(
                {
                    "id": message.id,
                    "received_at": format_datetime_for_display(message.received_at),
                    "text": text,
                    "contains_link": bool(message.contains_link),
                    "item_type": signals.get("item_type", "other"),
                    "primary_topic": signals.get("primary_topic", "overig"),
                    "importance_score": int(signals.get("importance_score", 0)),
                }
            )
        entries.sort(key=lambda item: (item.get("received_at") or "", int(item.get("id") or 0)))
        return entries

    def _build_full_resources_log(self, analyzed_resources: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        for item in analyzed_resources:
            resource: Resource = item["resource"]
            timestamp = ""
            raw_message = getattr(resource, "raw_message", None)
            if raw_message and raw_message.received_at:
                timestamp = format_datetime_for_display(raw_message.received_at)
            elif resource.fetched_at:
                timestamp = format_datetime_for_display(resource.fetched_at)

            description = (resource.description or resource.extracted_text or "").strip()
            entries.append(
                {
                    "id": resource.id,
                    "timestamp": timestamp,
                    "title": resource.title or resource.canonical_url or resource.url,
                    "url": resource.canonical_url or resource.final_url or resource.url,
                    "platform": resource.platform or "unknown",
                    "content_format": resource.content_format or "unknown",
                    "item_type": item.get("item_type", "resource"),
                    "primary_topic": item.get("primary_topic", "overig"),
                    "importance_score": int(item.get("importance_score", 0)),
                    "description": shorten(description, width=600, placeholder="…") if description else "",
                    "domain": resource.domain or "onbekend",
                    "extraction_status": resource.extraction_status or "pending",
                }
            )
        entries.sort(key=lambda entry: (entry.get("timestamp") or "", int(entry.get("id") or 0)))
        return entries

    def _links_overview(self, resources: Sequence[Resource]) -> Dict[str, List[Dict[str, str]]]:
        grouped: Dict[str, List[Dict[str, str]]] = {
            "artikelen": [],
            "videos": [],
            "social": [],
            "overig": [],
        }
        for resource in resources:
            item = {
                "title": resource.title or resource.canonical_url or resource.url,
                "domain": resource.domain or "onbekend",
                "url": resource.canonical_url or resource.final_url or resource.url,
            }
            content_format = (resource.content_format or "").lower()
            platform = (resource.platform or "").lower()
            if content_format in {"web_article", "generic_webpage", "linkedin_post"}:
                grouped["artikelen"].append(item)
            elif content_format in {"youtube_video", "youtube_short"} or platform == "youtube":
                grouped["videos"].append(item)
            elif platform in {"instagram", "linkedin"}:
                grouped["social"].append(item)
            else:
                grouped["overig"].append(item)
        return grouped

    def _is_strong_resource(self, resource: Resource) -> bool:
        status = (resource.extraction_status or "").lower()
        if status not in {"success", "partial"}:
            return False
        has_metadata = bool(resource.title or resource.description)
        extracted_len = len((resource.extracted_text or "").strip())
        return has_metadata or extracted_len >= 180

    def _is_noise_resource(self, resource: Resource) -> bool:
        url_value = (resource.canonical_url or resource.final_url or resource.url or "").strip().lower()
        if not url_value:
            return True
        title = (resource.title or "").strip().lower()
        description = (resource.description or "").strip().lower()
        extracted_text = (resource.extracted_text or "").strip()
        markers = {
            "redirect",
            "moved permanently",
            "just a moment",
            "access denied",
            "not found",
            "forbidden",
        }
        if any(marker in title for marker in markers):
            return True
        if any(marker in description for marker in markers):
            return True
        if not title and not description and len(extracted_text) < 80:
            return True
        return False

    def _resource_dedupe_key(self, resource: Resource) -> str:
        raw_url = (resource.canonical_url or resource.final_url or resource.url or "").strip()
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
                or key.lower()
                in {
                    "igsh",
                    "si",
                    "feature",
                    "fbclid",
                    "gclid",
                    "mc_cid",
                    "mc_eid",
                    "trk",
                    "trackingid",
                }
            )
        ]
        filtered_qs.sort()
        normalized_query = urlencode(filtered_qs, doseq=True)

        if is_linkedin_post:
            key = f"linkedin_post::{domain}{path_full.rstrip('/') or '/'}"
            if normalized_query:
                key = f"{key}?{normalized_query}"
            return key

        normalized = parsed._replace(
            netloc=domain,
            path=(path_full.rstrip("/") or "/"),
            query=normalized_query,
            fragment="",
        )
        return urlunparse(normalized)

    def _title_signature(self, value: Optional[str]) -> str:
        title = (value or "").strip().lower()
        if not title:
            return ""
        normalized = re.sub(r"\s+", " ", title)
        normalized = re.sub(r"[^a-z0-9 ]+", "", normalized)
        return normalized[:120]

    def _resource_why_relevant(self, resource: Resource, *, signal: Dict[str, Any]) -> str:
        topic = signal.get("primary_topic", "overig")
        item_type = signal.get("item_type", "resource")
        score = int(signal.get("importance_score", 0))
        content_format = (resource.content_format or "").lower()
        if content_format in {"youtube_short", "instagram_reel", "instagram_post"}:
            return f"Signaal van snelle consumptie rond '{topic}'; gebruik dit als trigger voor gerichte verdieping ({item_type}, score {score}/10)."
        if content_format in {"web_article", "generic_webpage", "linkedin_post", "youtube_video"}:
            return f"Inhoudelijk bruikbare bron rond '{topic}' die je direct kunt vertalen naar actie ({item_type}, score {score}/10)."
        return f"Relevant binnen '{topic}' en bruikbaar voor je volgende stap ({item_type}, score {score}/10)."

    def _render_email_body(
        self,
        *,
        subject: str,
        intro_summary: str,
        key_insight: str,
        highlights: Sequence[Dict[str, Any]],
        themes: Dict[str, Any],
        actions: Sequence[str],
        reflection: str,
        meta_analysis: str,
        all_messages: Sequence[Dict[str, Any]],
        all_resources: Sequence[Dict[str, Any]],
    ) -> str:
        lines: List[str] = [subject, "=" * len(subject), ""]

        lines.extend([
            "Intro / kernsamenvatting",
            "----------------------",
            intro_summary,
            f"Belangrijkste inzicht: {key_insight}",
            f"Voor dit overzicht zijn {int(themes.get('duplicates_filtered_count') or 0)} dubbele items verwijderd.",
            "",
        ])

        if highlights:
            lines.append("Highlights")
            lines.append("---------")
            for highlight in highlights:
                prefix = "Eigen idee: " if highlight.get("type") == "note" else ""
                lines.append(f"- {prefix}{highlight.get('title', '')}")
                lines.append(f"  {highlight.get('detail', '')}")
                lines.append(f"  Waarom relevant: {highlight.get('why_relevant', '')}")
                if highlight.get("url"):
                    lines.append(f"  {highlight['url']}")
            lines.append("")

        lines.append("Thema’s")
        lines.append("-------")
        for pattern in themes.get("patterns", [])[:5]:
            lines.append(f"- {pattern}")
        if themes.get("story"):
            lines.append(themes["story"])
        lines.append("")

        if actions:
            lines.append("Actiepunten")
            lines.append("-----------")
            for action in actions[:3]:
                lines.append(f"- {action}")
            lines.append("")

        lines.append("Reflectie")
        lines.append("---------")
        lines.extend(reflection.splitlines() if reflection else ["Geen reflectie beschikbaar."])
        lines.append("")

        lines.extend(self._render_plaintext_full_overview(all_messages, all_resources))

        lines.append("Meta-analyse")
        lines.append("------------")
        lines.append(meta_analysis)
        lines.append("")
        lines.append("Antwoord gerust met feedback of aanvullingen.")
        return "\n".join(lines)

    def _render_plaintext_full_overview(
        self,
        all_messages: Sequence[Dict[str, Any]],
        all_resources: Sequence[Dict[str, Any]],
    ) -> List[str]:
        lines: List[str] = []

        lines.append(f"Volledig overzicht: alle berichten ({len(all_messages)})")
        lines.append("----------------------------------")
        if all_messages:
            if len(all_messages) > 25:
                lines.append("Compacte weergave actief: berichten zijn per dag gegroepeerd met korte metadata.")
            for day, day_items in self._group_entries_by_day(all_messages, timestamp_key="received_at"):
                lines.append(f"{day}")
                for message in day_items:
                    time_value = self._time_from_display_timestamp(str(message.get("received_at") or ""))
                    lines.append(
                        f"- {time_value} | type={message.get('item_type', 'other')} "
                        f"| topic={message.get('primary_topic', 'overig')} "
                        f"| score={message.get('importance_score', 0)}/10"
                    )
                    lines.append(f"  {message.get('text', '')}")
                lines.append("")
        else:
            lines.append("- Geen berichten in deze periode.")
            lines.append("")

        lines.append(f"Volledig overzicht: alle links en bronnen ({len(all_resources)})")
        lines.append("-----------------------------------------")
        if all_resources:
            if len(all_resources) > 25:
                lines.append("Compacte weergave actief: bronnen zijn per dag gegroepeerd met compacte metadata.")
            for day, day_items in self._group_entries_by_day(all_resources, timestamp_key="timestamp"):
                lines.append(f"{day}")
                for resource in day_items:
                    time_value = self._time_from_display_timestamp(str(resource.get("timestamp") or ""))
                    lines.append(
                        f"- {time_value} | {resource.get('platform', 'unknown')}/{resource.get('content_format', 'unknown')} "
                        f"| type={resource.get('item_type', 'resource')} "
                        f"| topic={resource.get('primary_topic', 'overig')} "
                        f"| score={resource.get('importance_score', 0)}/10"
                    )
                    lines.append(f"  {resource.get('title', '')}")
                    if resource.get("url"):
                        lines.append(f"  {resource['url']}")
                    if resource.get("description"):
                        lines.append(f"  {resource['description']}")
                    status = resource.get("extraction_status")
                    if status and status != "success":
                        lines.append(f"  extractie-status: {status}")
                lines.append("")
        else:
            lines.append("- Geen bronnen in deze periode.")
            lines.append("")

        return lines

    def _group_entries_by_day(
        self,
        items: Sequence[Dict[str, Any]],
        *,
        timestamp_key: str,
    ) -> List[Tuple[str, List[Dict[str, Any]]]]:
        grouped: List[Tuple[str, List[Dict[str, Any]]]] = []
        current_day: Optional[str] = None
        current_items: List[Dict[str, Any]] = []

        for item in items:
            timestamp_value = str(item.get(timestamp_key) or "")
            day_value = self._day_from_display_timestamp(timestamp_value)
            if current_day is None:
                current_day = day_value
            if day_value != current_day:
                grouped.append((current_day, current_items))
                current_day = day_value
                current_items = [item]
            else:
                current_items.append(item)

        if current_day is not None:
            grouped.append((current_day, current_items))

        return grouped

    def _day_from_display_timestamp(self, value: str) -> str:
        if value and len(value) >= 10 and value[4:5] == "-" and value[7:8] == "-":
            return value[:10]
        return "Onbekende datum"

    def _time_from_display_timestamp(self, value: str) -> str:
        if value and len(value) >= 16 and value[4:5] == "-" and value[7:8] == "-" and value[10:11] == " ":
            return value[11:16]
        if value and len(value) >= 5:
            return value[-5:]
        return "--:--"



def _trend_text(delta: int, noun: str) -> str:
    if delta > 0:
        return f"steeg het aantal {noun} met {delta}"
    if delta < 0:
        return f"daalde het aantal {noun} met {abs(delta)}"
    return f"bleef het aantal {noun} gelijk"

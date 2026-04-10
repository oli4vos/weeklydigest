"""SMTP email service with plain-text + HTML rendering for digest reports."""
from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from html import escape
from textwrap import shorten
from typing import Any, Iterable, Sequence

from app.config import Settings, get_settings
from app.models import WeeklyReport


class EmailService:
    """SMTP client for onboarding mails and digest delivery."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.logger = logging.getLogger(__name__)

    def send_report(self, report: WeeklyReport, *, to_address: str | None = None) -> None:
        if not self.settings.smtp_host:
            raise RuntimeError("SMTP_HOST is not configured")
        recipient = to_address or self.settings.email_to
        if not recipient:
            raise RuntimeError("EMAIL_TO must be configured or provided via --to")

        msg = EmailMessage()
        msg["Subject"] = report.email_subject or "Weekly Digest"
        msg["From"] = self.settings.email_from or self.settings.smtp_username or "noreply@example.com"
        msg["To"] = recipient
        plain_body = report.email_body or "Geen inhoud beschikbaar."
        msg.set_content(plain_body)
        msg.add_alternative(self._render_html_body(report, plain_body), subtype="html")

        with smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port) as smtp:
            smtp.ehlo()
            if self.settings.smtp_port not in (25, 2525) and self.settings.smtp_host not in {"localhost", "127.0.0.1"}:
                smtp.starttls()
                smtp.ehlo()
            if self.settings.smtp_username and self.settings.smtp_password:
                smtp.login(self.settings.smtp_username, self.settings.smtp_password)
            smtp.send_message(msg)

        self.logger.info("Sent weekly report %s to %s", report.id, recipient)

    def send_verification_code(
        self,
        *,
        email_address: str,
        code: str,
        display_name: str | None = None,
    ) -> None:
        """Send one-time verification code during Telegram onboarding."""
        if not self.settings.smtp_host:
            raise RuntimeError("SMTP_HOST is not configured")
        if not email_address:
            raise RuntimeError("Verification email address is required")

        msg = EmailMessage()
        msg["Subject"] = "Je verificatiecode voor Knowledge Inbox"
        msg["From"] = self.settings.email_from or self.settings.smtp_username or "noreply@example.com"
        msg["To"] = email_address
        name = display_name or "gebruiker"

        plain = (
            f"Hoi {name},\n\n"
            f"Je verificatiecode is: {code}\n\n"
            "Deze code verloopt snel. Deel de code niet met anderen.\n"
        )
        msg.set_content(plain)
        msg.add_alternative(
            (
                "<!DOCTYPE html><html><body style='font-family:Arial,sans-serif;line-height:1.5;color:#111;'>"
                f"<p>Hoi {escape(name)},</p>"
                "<p>Je verificatiecode is:</p>"
                f"<p style='font-size:24px;font-weight:700;letter-spacing:3px;'>{escape(code)}</p>"
                "<p>Deze code verloopt snel. Deel de code niet met anderen.</p>"
                "</body></html>"
            ),
            subtype="html",
        )

        with smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port) as smtp:
            smtp.ehlo()
            if self.settings.smtp_port not in (25, 2525) and self.settings.smtp_host not in {"localhost", "127.0.0.1"}:
                smtp.starttls()
                smtp.ehlo()
            if self.settings.smtp_username and self.settings.smtp_password:
                smtp.login(self.settings.smtp_username, self.settings.smtp_password)
            smtp.send_message(msg)

        self.logger.info("Sent onboarding verification email to user destination")

    def _render_html_body(self, report: WeeklyReport, plain_body: str) -> str:
        """Render scanable, single-column digest HTML."""
        highlights = self._ensure_list(report.highlights_json)
        themes = report.themes_json or {}
        actions = [str(item) for item in self._ensure_list(report.actions_json) if item]
        full_overview = themes.get("full_overview") if isinstance(themes.get("full_overview"), dict) else {}
        all_messages = self._ensure_list(full_overview.get("messages") or themes.get("all_messages"))
        all_resources = self._ensure_list(full_overview.get("resources") or themes.get("all_resources"))

        summary_text = f"{report.source_message_count} berichten · {report.source_resource_count} bronnen"
        if themes.get("story"):
            summary_text += f" — {themes['story']}"

        duplicates_filtered_count = int(themes.get("duplicates_filtered_count") or 0)
        key_insight = self._derive_key_insight(report, themes)

        sections: list[str] = [
            f"<h1 style='{self._heading_style(level=1)}'>Weekly Digest</h1>",
            self._render_summary_card(summary_text, duplicates_filtered_count),
            self._render_key_insight(key_insight),
        ]

        highlights_html = self._render_highlights(highlights)
        if highlights_html:
            sections.append(highlights_html)

        themes_html = self._render_themes(themes)
        if themes_html:
            sections.append(themes_html)

        actions_html = self._render_action_block(actions)
        if actions_html:
            sections.append(actions_html)

        reflection_html = self._render_reflection(report.reflection or "")
        if reflection_html:
            sections.append(reflection_html)

        sections.append(self._render_full_notes_section(all_messages))
        sections.append(self._render_full_resources_section(all_resources))

        if report.meta_analysis:
            sections.append(
                "<div style='margin-top:20px;padding:12px;border:1px solid #e5e7eb;border-radius:10px;background:#fafafa;'>"
                "<h2 style='margin:0 0 8px 0;font-size:16px;'>Meta-analyse</h2>"
                f"<p style='margin:0;color:#334155;'>{escape(report.meta_analysis)}</p>"
                "</div>"
            )

        # Keep raw text fallback visible for debugging/support.
        sections.append(
            "<hr style='margin:28px 0;border:none;border-top:1px solid #e5e7eb;'/>"
            "<p style='margin:0 0 8px 0;color:#64748b;font-size:12px;'>Tekstversie</p>"
            "<pre style='margin:0;background:#f8fafc;padding:12px;border-radius:8px;white-space:pre-wrap;"
            "font-family:SFMono-Regular,Consolas,monospace;font-size:12px;line-height:1.5;'>"
            f"{escape(plain_body)}"
            "</pre>"
        )

        container_style = (
            "max-width:640px;margin:0 auto;background:#ffffff;padding:24px;"
            "font-family:Arial,sans-serif;line-height:1.6;color:#111111;"
        )
        return (
            "<!DOCTYPE html><html><body style='margin:0;padding:16px;background:#f3f4f6;'>"
            f"<div style='{container_style}'>"
            + "".join(sections)
            + "</div></body></html>"
        )

    def _render_summary_card(self, summary_text: str, duplicates_filtered_count: int) -> str:
        return (
            "<div style='padding:14px 16px;border:1px solid #e5e7eb;border-radius:10px;background:#fafafa;margin-bottom:14px;'>"
            f"<p style='margin:0 0 8px 0;'><strong>Samenvatting:</strong> {escape(summary_text)}</p>"
            f"<p style='margin:0;color:#334155;'>Voor dit overzicht zijn {duplicates_filtered_count} dubbele items verwijderd.</p>"
            "</div>"
        )

    def _render_key_insight(self, insight: str) -> str:
        return (
            "<div style='padding:14px 16px;border-radius:10px;background:#eef2ff;margin-bottom:18px;'>"
            "<p style='margin:0;font-size:14px;'><strong>Belangrijkste inzicht</strong><br/>"
            f"{escape(insight)}</p>"
            "</div>"
        )

    def _render_highlights(self, highlights: Sequence[dict[str, Any]]) -> str:
        if not highlights:
            return ""

        items: list[str] = []
        for item in highlights[:5]:
            title = escape(str(item.get("title") or "Onbekende highlight"))
            detail = escape(str(item.get("detail") or ""))
            why_relevant = escape(str(item.get("why_relevant") or ""))
            url = str(item.get("url") or "")
            url_html = (
                f"<div style='margin-top:6px;'><a style='color:#2563eb;text-decoration:none;' href='{escape(url)}'>{escape(url)}</a></div>"
                if url
                else ""
            )
            items.append(
                "<li style='margin-bottom:12px;'>"
                f"<div style='font-weight:600;'>{title}</div>"
                f"<div style='font-size:13px;color:#475569;'>{detail}</div>"
                f"<div style='margin-top:4px;color:#1f2937;'>{why_relevant}</div>"
                f"{url_html}"
                "</li>"
            )

        return (
            f"<h2 style='{self._heading_style()}'>🔥 Highlights</h2>"
            "<ul style='margin:0 0 10px 0;padding-left:20px;'>"
            + "".join(items)
            + "</ul>"
        )

    def _render_themes(self, themes: dict[str, Any]) -> str:
        points: list[str] = []
        for pattern in self._ensure_list(themes.get("patterns"))[:5]:
            if pattern:
                points.append(escape(str(pattern)))

        top_topics = self._ensure_list(themes.get("top_topics"))
        if top_topics:
            points.append(escape("Top topics: " + ", ".join(str(item) for item in top_topics[:5])))

        item_types = self._ensure_list(themes.get("item_types"))
        if item_types:
            formatted = ", ".join(f"{name} ({count})" for name, count in item_types[:5])
            points.append(escape(f"Top item types: {formatted}"))

        thinking_count = int(themes.get("thinking_count") or 0)
        consumption_count = int(themes.get("consumption_count") or 0)
        if thinking_count or consumption_count:
            points.append(escape(f"Thinking vs consumptie: {thinking_count} vs {consumption_count}"))

        short_count = int(themes.get("short_form_count") or 0)
        long_count = int(themes.get("long_form_count") or 0)
        if short_count or long_count:
            points.append(escape(f"Short-form vs long-form: {short_count} vs {long_count}"))

        if not points:
            return ""

        return (
            f"<h2 style='{self._heading_style()}'>🧭 Thema’s</h2>"
            "<ul style='margin:0 0 10px 0;padding-left:20px;'>"
            + "".join(f"<li style='margin-bottom:8px;'>{point}</li>" for point in points)
            + "</ul>"
        )

    def _render_action_block(self, actions: Sequence[str]) -> str:
        cleaned = [escape(shorten(str(item), width=220, placeholder="…")) for item in actions if item]
        if not cleaned:
            return ""

        return (
            f"<h2 style='{self._heading_style()}'>⚡ Actiepunten</h2>"
            "<ol style='margin:0 0 10px 0;padding-left:20px;'>"
            + "".join(f"<li style='margin-bottom:8px;'>{action}</li>" for action in cleaned[:3])
            + "</ol>"
        )

    def _render_reflection(self, reflection: str) -> str:
        lines = [line.strip() for line in reflection.splitlines() if line.strip()]
        if not lines:
            return ""

        return (
            f"<h2 style='{self._heading_style()}'>🧠 Reflectie</h2>"
            "<div style='border:1px solid #e5e7eb;border-radius:10px;padding:12px 14px;background:#fcfcfd;'>"
            + "".join(f"<p style='margin:0 0 8px 0;color:#1f2937;'>{escape(line)}</p>" for line in lines)
            + "</div>"
        )

    def _render_full_notes_section(self, messages: Sequence[dict[str, Any]]) -> str:
        header = f"<h2 style='{self._heading_style()}'>🗂️ Volledig overzicht: alle berichten ({len(messages)})</h2>"
        if not messages:
            return header + "<p style='margin:0;'>Geen berichten in deze periode.</p>"

        sections: list[str] = [header]
        if len(messages) > 30:
            sections.append(
                "<p style='margin:0 0 10px 0;color:#64748b;font-size:12px;'>"
                "Compacte weergave actief: alle berichten zijn per dag gegroepeerd.</p>"
            )

        for day, day_items in self._group_items_by_day(messages, timestamp_key="received_at"):
            sections.append(
                f"<h3 style='margin:16px 0 8px 0;font-size:14px;color:#111827;border-bottom:1px solid #e5e7eb;padding-bottom:4px;'>{escape(day)}</h3>"
            )
            sections.append("<ul style='margin:0;padding:0;list-style:none;'>")
            for message in day_items:
                metadata = self._render_compact_metadata_line(
                    [
                        self._time_from_timestamp_value(str(message.get("received_at") or "")),
                        f"type={message.get('item_type', 'other')}",
                        f"topic={message.get('primary_topic', 'overig')}",
                        f"score={message.get('importance_score', 0)}/10",
                    ]
                )
                text_value = str(message.get("text") or "")
                content = escape(text_value)
                sections.append(
                    "<li style='padding:8px 0;border-bottom:1px solid #f1f5f9;'>"
                    f"<div style='font-size:12px;color:#64748b;margin-bottom:4px;'>{metadata}</div>"
                    f"<div style='white-space:pre-wrap;color:#111827;font-size:14px;'>{content}</div>"
                    "</li>"
                )
            sections.append("</ul>")

        return "".join(sections)

    def _render_full_resources_section(self, resources: Sequence[dict[str, Any]]) -> str:
        header = f"<h2 style='{self._heading_style()}'>🔗 Volledig overzicht: alle links en bronnen ({len(resources)})</h2>"
        if not resources:
            return header + "<p style='margin:0;'>Geen bronnen in deze periode.</p>"

        sections: list[str] = [header]
        if len(resources) > 30:
            sections.append(
                "<p style='margin:0 0 10px 0;color:#64748b;font-size:12px;'>"
                "Compacte weergave actief: alle bronnen zijn per dag gegroepeerd.</p>"
            )

        for day, day_items in self._group_items_by_day(resources, timestamp_key="timestamp"):
            sections.append(
                f"<h3 style='margin:16px 0 8px 0;font-size:14px;color:#111827;border-bottom:1px solid #e5e7eb;padding-bottom:4px;'>{escape(day)}</h3>"
            )
            sections.append("<ul style='margin:0;padding:0;list-style:none;'>")
            for resource in day_items:
                metadata = self._render_compact_metadata_line(
                    [
                        self._time_from_timestamp_value(str(resource.get("timestamp") or "")),
                        f"{resource.get('platform', 'unknown')}/{resource.get('content_format', 'unknown')}",
                        f"type={resource.get('item_type', 'resource')}",
                        f"topic={resource.get('primary_topic', 'overig')}",
                        f"score={resource.get('importance_score', 0)}/10",
                    ]
                )
                title = escape(str(resource.get("title") or "Onbekende bron"))
                url = str(resource.get("url") or "")
                description = str(resource.get("description") or "")
                description_text = escape(shorten(description, width=360, placeholder="…")) if description else ""
                status = str(resource.get("extraction_status") or "").strip().lower()
                status_line = (
                    f"<div style='margin-top:4px;font-size:12px;color:#7c2d12;'>extractie-status: {escape(status)}</div>"
                    if status and status != "success"
                    else ""
                )
                url_html = (
                    f"<div style='margin-top:4px;'><a style='color:#2563eb;text-decoration:none;' href='{escape(url)}'>{escape(url)}</a></div>"
                    if url
                    else ""
                )
                sections.append(
                    "<li style='padding:8px 0;border-bottom:1px solid #f1f5f9;'>"
                    f"<div style='font-size:12px;color:#64748b;margin-bottom:4px;'>{metadata}</div>"
                    f"<div style='font-weight:600;color:#111827;'>{title}</div>"
                    f"{url_html}"
                    f"<div style='margin-top:4px;color:#334155;font-size:13px;'>{description_text}</div>"
                    f"{status_line}"
                    "</li>"
                )
            sections.append("</ul>")

        return "".join(sections)

    def _group_items_by_day(
        self,
        items: Sequence[dict[str, Any]],
        *,
        timestamp_key: str,
    ) -> list[tuple[str, list[dict[str, Any]]]]:
        grouped: list[tuple[str, list[dict[str, Any]]]] = []
        current_day: str | None = None
        current_items: list[dict[str, Any]] = []

        for item in items:
            timestamp_value = str(item.get(timestamp_key) or "")
            day_value = self._day_from_timestamp_value(timestamp_value)
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

    def _day_from_timestamp_value(self, value: str) -> str:
        if value and len(value) >= 10 and value[4:5] == "-" and value[7:8] == "-":
            return value[:10]
        return "Onbekende datum"

    def _time_from_timestamp_value(self, value: str) -> str:
        if value and len(value) >= 16 and value[10:11] == " ":
            return value[11:16]
        if value and len(value) >= 5:
            return value[-5:]
        return "--:--"

    def _render_compact_metadata_line(self, values: Iterable[str]) -> str:
        cleaned = [escape(value.strip()) for value in values if value and value.strip()]
        return " · ".join(cleaned)

    def _derive_key_insight(self, report: WeeklyReport, themes: dict[str, Any]) -> str:
        if report.reflection:
            lines = [line.strip() for line in report.reflection.splitlines() if line.strip()]
            if lines:
                return lines[0]
        if themes.get("story"):
            return str(themes["story"])
        return "Kies één inzicht uit dit overzicht en zet het vandaag om in een concrete actie."

    @staticmethod
    def _ensure_list(value: Any) -> list:
        if isinstance(value, list):
            return value
        if value is None:
            return []
        return [value]

    @staticmethod
    def _heading_style(level: int = 2) -> str:
        font_size = "24px" if level == 1 else "19px"
        margin_top = "0" if level == 1 else "26px"
        return f"font-size:{font_size};margin-top:{margin_top};margin-bottom:12px;color:#111827;"

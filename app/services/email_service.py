"""Simple SMTP email service."""
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
    """Minimal SMTP client for sending weekly digests."""

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

    def _render_html_body(self, report: WeeklyReport, plain_body: str) -> str:
        """Render a friendly HTML digest for Gmail/Apple Mail."""
        highlights = self._ensure_list(report.highlights_json)
        ideas = self._ensure_list(report.ideas_json)
        actions = self._ensure_list(report.actions_json)
        themes = report.themes_json or {}

        summary_text = f"{report.source_message_count} berichten · {report.source_resource_count} bronnen"
        if themes.get("story"):
            summary_text += f" — {themes['story']}"

        primary_insight = report.reflection or themes.get("platform_story") or report.meta_analysis or summary_text

        sections: list[str] = []
        sections.append(f"<h1 style='{self._heading_style(level=1)}'>Weekly Digest</h1>")
        sections.append(f"<p style='{self._paragraph_style()}'><strong>Samenvatting:</strong> {escape(summary_text)}</p>")
        sections.append(
            "<div style='background:#f3f4f6;padding:12px 16px;border-radius:8px;margin:16px 0;'>"
            "<strong>Belangrijkste inzicht:</strong><br/>"
            f"{escape(primary_insight)}"
            "</div>"
        )

        highlights_html = self._render_highlights(highlights)
        if highlights_html:
            sections.append(highlights_html)

        themes_html = self._render_themes(themes, report.meta_analysis)
        if themes_html:
            sections.append(themes_html)

        ideas_html = self._render_ideas(ideas)
        if ideas_html:
            sections.append(ideas_html)

        actions_html = self._render_actions(actions)
        if actions_html:
            sections.append(actions_html)

        if report.reflection:
            sections.append(f"<h2 style='{self._heading_style()}'>🧠 Reflectie</h2>")
            sections.append(f"<p style='{self._paragraph_style()}'>{escape(report.reflection)}</p>")

        if report.meta_analysis:
            sections.append(f"<p style='{self._paragraph_style()}'><em>{escape(report.meta_analysis)}</em></p>")

        # Plain-text fallback preview
        sections.append(
            "<hr style='margin:32px 0;border:none;border-top:1px solid #e5e7eb;'/>"
            "<p style='color:#6b7280;font-size:13px;'>Tekstversie:</p>"
            f"<pre style='background:#f8fafc;padding:12px;border-radius:6px;white-space:pre-wrap;"
            f"font-family:SFMono-Regular,Consolas,\"Liberation Mono\",monospace;font-size:13px;'>"
            f"{escape(plain_body)}</pre>"
        )

        container_style = (
            "max-width:600px;margin:0 auto;background:#ffffff;padding:24px;"
            "font-family:Arial,sans-serif;line-height:1.6;color:#111111;"
        )
        return (
            "<!DOCTYPE html><html><body style='margin:0;padding:0;background:#f2f4f7;'>"
            f"<div style='{container_style}'>"
            + "".join(sections)
            + "</div></body></html>"
        )

    def _render_highlights(self, highlights: Sequence[dict[str, Any]]) -> str:
        if not highlights:
            return ""
        items: list[str] = []
        for hl in highlights:
            title = escape(hl.get("title") or "Onbekende highlight")
            detail = escape(shorten(hl.get("detail") or "", width=140, placeholder="…"))
            url = hl.get("url")
            link_html = (
                f"<br/><a style='color:#2563eb;text-decoration:none;' href='{escape(url)}'>{escape(url)}</a>"
                if url
                else ""
            )
            items.append(
                "<li style='margin-bottom:12px;'>"
                f"<strong>{title}</strong><br/>{detail}{link_html}"
                "</li>"
            )
        return (
            f"<h2 style='{self._heading_style()}'>🔥 Highlights</h2>"
            "<ul style='padding-left:20px;margin:0;'>"
            + "".join(items)
            + "</ul>"
        )

    def _render_themes(self, themes: dict[str, Any], meta_analysis: str | None) -> str:
        bullets: list[str] = []
        if themes.get("story"):
            bullets.append(escape(themes["story"]))
        if themes.get("platform_story"):
            bullets.append(escape(themes["platform_story"]))
        if meta_analysis:
            bullets.append(escape(meta_analysis))
        if not bullets:
            return ""
        return (
            f"<h2 style='{self._heading_style()}'>🧭 Thema’s</h2>"
            "<ul style='padding-left:20px;margin:0;'>"
            + "".join(f"<li style='margin-bottom:8px;'>{point}</li>" for point in bullets)
            + "</ul>"
        )

    def _render_ideas(self, ideas: Sequence[dict[str, Any]]) -> str:
        snippets: list[str] = []
        for idea in ideas:
            text = (idea.get("text") or "").strip()
            if not text:
                continue
            snippet = escape(shorten(text, width=160, placeholder="…"))
            timestamp = idea.get("received_at")
            meta = f"<br/><small style='color:#6b7280;'>{escape(timestamp)}</small>" if timestamp else ""
            snippets.append(f"<li style='margin-bottom:10px;'>{snippet}{meta}</li>")
        if not snippets:
            return ""
        return (
            f"<h2 style='{self._heading_style()}'>💡 Eigen ideeën</h2>"
            "<ul style='padding-left:20px;margin:0;'>"
            + "".join(snippets)
            + "</ul>"
        )

    def _render_actions(self, actions: Sequence[str]) -> str:
        clean_actions = [escape(shorten(str(action), width=160, placeholder="…")) for action in actions if action]
        if not clean_actions:
            return ""
        return (
            f"<h2 style='{self._heading_style()}'>⚡ Actiepunten</h2>"
            "<ol style='padding-left:20px;margin:0;'>"
            + "".join(f"<li style='margin-bottom:8px;'>{action}</li>" for action in clean_actions)
            + "</ol>"
        )

    @staticmethod
    def _ensure_list(value: Any) -> list:
        if isinstance(value, list):
            return value
        if value is None:
            return []
        return [value]

    @staticmethod
    def _heading_style(level: int = 2) -> str:
        font_size = "22px" if level == 1 else "18px"
        margin_top = "0" if level == 1 else "24px"
        return f"font-size:{font_size};margin-top:{margin_top};margin-bottom:12px;color:#111111;"

    @staticmethod
    def _paragraph_style() -> str:
        return "margin:0 0 12px 0;color:#111111;"

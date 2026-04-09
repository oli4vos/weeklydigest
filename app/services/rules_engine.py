"""Deterministic bilingual intelligence layer for digest-quality signals."""
from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # avoid runtime model import side-effects in utility-only contexts
    from app.models import Resource
else:  # pragma: no cover
    Resource = Any


TOPIC_RULES: Dict[str, List[str]] = {
    "ai": [
        "ai",
        "kunstmatige intelligentie",
        "llm",
        "gpt",
        "openai",
        "anthropic",
        "claude",
        "model",
        "foundation model",
        "agent",
        "agents",
        "prompt",
        "prompting",
        "prompt engineering",
        "fine tuning",
        "finetune",
        "rag",
        "retrieval",
        "embedding",
        "embeddings",
        "vector",
        "vector database",
        "inference",
        "hallucination",
        "alignment",
        "ai tool",
        "ai tools",
        "automation ai",
    ],
    "legal_tech": [
        "legal",
        "law",
        "juridisch",
        "wet",
        "regelgeving",
        "compliance",
        "contract",
        "contracten",
        "voorwaarden",
        "terms",
        "privacy",
        "gdpr",
        "avg",
        "compliance officer",
        "legal tech",
        "document review",
        "liability",
        "aansprakelijkheid",
        "regulation",
        "regel",
        "wetgeving",
    ],
    "productivity": [
        "productivity",
        "productiviteit",
        "focus",
        "deep work",
        "planning",
        "workflow",
        "system",
        "systeem",
        "routine",
        "habit",
        "gewoonte",
        "time management",
        "timemanagement",
        "priorities",
        "prioriteiten",
        "efficiency",
        "efficientie",
        "discipline",
        "output",
        "execution",
    ],
    "knowledge_management": [
        "knowledge",
        "kennis",
        "notities",
        "notes",
        "note taking",
        "second brain",
        "zettelkasten",
        "pkm",
        "personal knowledge",
        "samenvatting",
        "summary",
        "bibliotheek",
        "library",
        "opslaan",
        "capture",
        "collect",
        "retrieve",
        "organize",
        "organiseren",
        "linking",
        "denken",
        "thought system",
    ],
    "business": [
        "business",
        "bedrijf",
        "startup",
        "scaleup",
        "ondernemen",
        "entrepreneur",
        "ondernemer",
        "revenue",
        "omzet",
        "profit",
        "winst",
        "margin",
        "strategie",
        "strategy",
        "growth",
        "groei",
        "market",
        "markt",
        "positioning",
        "positionering",
    ],
    "marketing": [
        "marketing",
        "branding",
        "brand",
        "content marketing",
        "content",
        "social media",
        "linkedin",
        "instagram",
        "twitter",
        "x.com",
        "growth hacking",
        "seo",
        "ads",
        "advertising",
        "copywriting",
        "copy",
        "funnel",
        "conversion",
        "conversie",
        "audience",
        "publiek",
    ],
    "sales": [
        "sales",
        "verkoop",
        "closing",
        "lead",
        "leads",
        "pipeline",
        "prospect",
        "deal",
        "crm",
        "client",
        "klant",
        "customer",
        "negotiation",
        "onderhandeling",
    ],
    "finance": [
        "finance",
        "financien",
        "geld",
        "money",
        "investing",
        "investeren",
        "crypto",
        "bitcoin",
        "stocks",
        "aandelen",
        "valuation",
        "waardering",
        "cashflow",
        "kosten",
        "expense",
        "budget",
        "budgeting",
    ],
    "health": [
        "health",
        "gezondheid",
        "fitness",
        "workout",
        "training",
        "gym",
        "voeding",
        "nutrition",
        "sleep",
        "slaap",
        "stress",
        "mental health",
        "mentaal",
        "energie",
        "energy",
        "biohacking",
    ],
    "psychology": [
        "psychology",
        "psychologie",
        "mindset",
        "thinking",
        "gedrag",
        "behavior",
        "cognition",
        "bias",
        "biases",
        "decision making",
        "besluitvorming",
        "emotions",
        "emoties",
        "attention",
    ],
    "philosophy": [
        "philosophy",
        "filosofie",
        "ethics",
        "ethiek",
        "meaning",
        "zin",
        "purpose",
        "doel",
        "existential",
        "existentie",
        "stoicism",
        "stoicisme",
        "waarheid",
    ],
    "programming": [
        "programming",
        "coding",
        "code",
        "developer",
        "software",
        "backend",
        "frontend",
        "python",
        "javascript",
        "typescript",
        "api",
        "framework",
        "library",
        "bug",
        "debug",
        "deploy",
        "deployment",
    ],
    "tools": [
        "tool",
        "tools",
        "software",
        "app",
        "platform",
        "notion",
        "obsidian",
        "slack",
        "zapier",
        "automation tool",
        "workflow tool",
        "ai tool",
        "saas",
        "software as a service",
    ],
    "automation": [
        "automation",
        "automatisering",
        "zapier",
        "make.com",
        "workflow automation",
        "scripts",
        "cron",
        "bot",
        "bots",
        "pipeline",
        "process automation",
    ],
    "design": [
        "design",
        "ux",
        "ui",
        "user experience",
        "interface",
        "visual",
        "typography",
        "layout",
        "design system",
    ],
    "media": [
        "video",
        "youtube",
        "podcast",
        "article",
        "artikel",
        "blog",
        "content",
        "reel",
        "short",
        "media",
        "news",
        "nieuws",
    ],
    "learning": [
        "learning",
        "leren",
        "study",
        "studeren",
        "course",
        "cursus",
        "education",
        "opleiding",
        "skill",
        "vaardigheid",
        "training",
        "practice",
        "oefenen",
    ],
    "career": [
        "career",
        "carriere",
        "job",
        "baan",
        "werk",
        "salary",
        "salaris",
        "promotion",
        "ontwikkeling",
        "cv",
        "resume",
    ],
    "networking": [
        "networking",
        "netwerk",
        "connecties",
        "connections",
        "linkedin",
        "relationship",
        "relatie",
        "community",
    ],
    "ideas": ["idee", "ideeen", "idea", "concept", "brainstorm", "inspiration", "inspiratie", "creativity", "creativiteit"],
    "tasks": ["todo", "to do", "task", "taak", "ik moet", "onthouden", "reminder", "doen", "actie", "actiepunt"],
    "reflection": [
        "reflectie",
        "reflection",
        "ik denk",
        "ik merk",
        "inzicht",
        "lesson",
        "observatie",
        "opmerking",
        "wat ik leer",
        "wat ik zie",
    ],
    "trends": ["trend", "trends", "ontwikkeling", "development", "future", "toekomst", "emerging", "nieuw", "innovatie"],
    "startups": ["startup", "founder", "vc", "venture capital", "pitch", "scale", "scaling", "growth"],
    "economics": ["economics", "economie", "inflation", "inflatie", "market", "markt", "supply", "demand", "vraag", "aanbod"],
    "technology": ["technology", "tech", "software", "hardware", "innovation", "innovatie", "internet", "web", "platform"],
}

QUESTION_SIGNALS = [
    "hoe",
    "wat",
    "waarom",
    "wanneer",
    "welke",
    "which",
    "what",
    "why",
    "how",
    "when",
    "kan ik",
    "should i",
]
TASK_SIGNALS = [
    "ik moet",
    "moet ik",
    "todo",
    "to do",
    "taak",
    "onthouden",
    "reminder",
    "next step",
    "volgende stap",
    "need to",
    "actiepunt",
]
IDEA_SIGNALS = ["idee", "ideeen", "idea", "misschien", "zou kunnen", "concept", "brainstorm", "prototype"]
REFLECTION_SIGNALS = ["reflectie", "reflection", "ik merk", "ik denk", "ik voel", "geleerd", "lesson", "inzicht", "patroon"]
INSPIRATION_SIGNALS = ["inspiratie", "inspiration", "quote", "trend", "nieuw perspectief", "interesting", "mooi"]

SHORT_FORMATS = {"youtube_short", "instagram_reel", "instagram_post"}
LONG_FORMATS = {"web_article", "generic_webpage", "linkedin_post", "youtube_video"}
THINKING_TYPES = {"idea", "task", "question", "reflection", "note"}


@dataclass
class RuleSignals:
    primary_topic: str
    matched_topics: List[str]
    item_type: str
    importance_score: int
    is_consumption: bool
    is_thinking: bool
    content_depth: str

    def as_dict(self) -> Dict[str, object]:
        return {
            "primary_topic": self.primary_topic,
            "matched_topics": self.matched_topics,
            "item_type": self.item_type,
            "importance_score": self.importance_score,
            "is_consumption": self.is_consumption,
            "is_thinking": self.is_thinking,
            "content_depth": self.content_depth,
        }


class RulesEngine:
    """Rule-based classifier/scorer for deterministic digest intelligence."""

    def analyze_note(self, text: str, *, has_link: bool = False) -> RuleSignals:
        content = (text or "").strip()
        matched_topics = self._classify_topics(content)
        item_type = self._classify_item_type(content, has_link=has_link, is_resource=False)
        content_depth = self._classify_depth(content_format="plain_text", text_length=len(content))
        is_thinking = item_type in THINKING_TYPES or not has_link
        is_consumption = bool(has_link and not is_thinking)
        score = self._importance_score(
            text_length=len(content),
            item_type=item_type,
            is_note=True,
            extraction_status=None,
            has_rich_metadata=len(content) >= 120,
            topic_count=len(matched_topics),
            topic_frequency=1,
            is_duplicate=False,
            is_weak_link=False,
            is_redirect=False,
            is_consumption=is_consumption,
            is_thinking=is_thinking,
            content_depth=content_depth,
        )
        return RuleSignals(
            primary_topic=matched_topics[0] if matched_topics else "knowledge_management",
            matched_topics=matched_topics,
            item_type=item_type,
            importance_score=score,
            is_consumption=is_consumption,
            is_thinking=is_thinking,
            content_depth=content_depth,
        )

    def analyze_resource(self, resource: Resource, *, related_text: str = "") -> RuleSignals:
        combined = self._resource_text(resource, related_text=related_text)
        matched_topics = self._classify_topics(combined)
        content_format = (resource.content_format or "").lower()
        item_type = self._classify_item_type(combined, has_link=True, is_resource=True, content_format=content_format)
        text_len = len(combined)
        content_depth = self._classify_depth(content_format=content_format, text_length=text_len)
        is_thinking = item_type in THINKING_TYPES and text_len >= 80
        is_consumption = not is_thinking
        extraction_status = (resource.extraction_status or "").lower()
        has_rich_metadata = bool(resource.title or resource.description) or len((resource.extracted_text or "").strip()) >= 220
        is_weak_link = not has_rich_metadata and extraction_status in {"failed", "partial"}
        is_redirect = self._is_redirect_resource(resource)
        score = self._importance_score(
            text_length=text_len,
            item_type=item_type,
            is_note=False,
            extraction_status=extraction_status,
            has_rich_metadata=has_rich_metadata,
            topic_count=len(matched_topics),
            topic_frequency=1,
            is_duplicate=False,
            is_weak_link=is_weak_link,
            is_redirect=is_redirect,
            is_consumption=is_consumption,
            is_thinking=is_thinking,
            content_depth=content_depth,
        )
        return RuleSignals(
            primary_topic=matched_topics[0] if matched_topics else "media",
            matched_topics=matched_topics,
            item_type=item_type,
            importance_score=score,
            is_consumption=is_consumption,
            is_thinking=is_thinking,
            content_depth=content_depth,
        )

    def apply_topic_recurrence_boost(self, items: List[Dict[str, Any]], topic_counts: Counter) -> None:
        """Boost item importance for recurring topics without changing schema/storage."""
        for item in items:
            topic = item.get("primary_topic")
            if not topic:
                continue
            frequency = int(topic_counts.get(topic, 0))
            bonus = 2 if frequency >= 3 else 1 if frequency == 2 else 0
            if bonus:
                item["importance_score"] = max(0, min(10, int(item.get("importance_score", 0)) + bonus))
            item["topic_frequency"] = frequency

    def _classify_topics(self, text: str) -> List[str]:
        haystack = f" {text.lower()} "
        scores: Dict[str, int] = {}
        for topic, keywords in TOPIC_RULES.items():
            score = 0
            for keyword in keywords:
                kw = keyword.strip().lower()
                if not kw:
                    continue
                if " " in kw:
                    score += haystack.count(kw) * 2
                else:
                    score += len(re.findall(rf"\b{re.escape(kw)}\b", haystack))
            if score > 0:
                scores[topic] = score
        ranked = sorted(scores.items(), key=lambda entry: entry[1], reverse=True)
        return [topic for topic, _ in ranked[:4]]

    def _classify_item_type(self, text: str, *, has_link: bool, is_resource: bool, content_format: str = "") -> str:
        value = (text or "").lower()
        if "?" in value or self._contains_any(value, QUESTION_SIGNALS):
            return "question"
        if self._contains_any(value, TASK_SIGNALS):
            return "task"
        if self._contains_any(value, IDEA_SIGNALS):
            return "idea"
        if self._contains_any(value, REFLECTION_SIGNALS):
            return "reflection"
        if self._contains_any(value, INSPIRATION_SIGNALS):
            return "inspiration"
        if is_resource or has_link:
            return "resource"
        if len(value) >= 320:
            return "reflection"
        if len(value) >= 80:
            return "note"
        return "other"

    def _classify_depth(self, *, content_format: str, text_length: int) -> str:
        if content_format in SHORT_FORMATS:
            return "short_form"
        if content_format in LONG_FORMATS:
            return "long_form"
        if text_length >= 450:
            return "long_form"
        if 0 < text_length <= 120:
            return "short_form"
        return "unknown"

    def _importance_score(
        self,
        *,
        text_length: int,
        item_type: str,
        is_note: bool,
        extraction_status: Optional[str],
        has_rich_metadata: bool,
        topic_count: int,
        topic_frequency: int,
        is_duplicate: bool,
        is_weak_link: bool,
        is_redirect: bool,
        is_consumption: bool,
        is_thinking: bool,
        content_depth: str,
    ) -> int:
        points = 0
        if is_note:
            points += 3
        if item_type in {"idea", "task", "reflection"}:
            points += 3
        elif item_type == "question":
            points += 2
        elif item_type in {"inspiration", "note"}:
            points += 1
        if text_length >= 120:
            points += 1
        if text_length >= 280:
            points += 1
        if has_rich_metadata:
            points += 2
        if extraction_status == "success":
            points += 2
        elif extraction_status == "partial":
            points += 1
        elif extraction_status == "failed":
            points -= 2
        if topic_count >= 2:
            points += 1
        if topic_frequency >= 3:
            points += 2
        elif topic_frequency == 2:
            points += 1
        if is_thinking:
            points += 2
        if is_consumption:
            points -= 1
        if content_depth == "long_form":
            points += 1
        if content_depth == "short_form" and is_consumption:
            points -= 1
        if is_weak_link:
            points -= 2
        if is_redirect:
            points -= 1
        if is_duplicate:
            points -= 2
        return max(0, min(10, points))

    @staticmethod
    def _contains_any(text: str, words: List[str]) -> bool:
        return any(word in text for word in words)

    @staticmethod
    def _resource_text(resource: Resource, *, related_text: str = "") -> str:
        parts = [
            (related_text or "").strip(),
            (resource.title or "").strip(),
            (resource.description or "").strip(),
            (resource.extracted_text or "")[:1200].strip(),
        ]
        return " ".join(part for part in parts if part)

    @staticmethod
    def _is_redirect_resource(resource: Resource) -> bool:
        original = (resource.url or "").strip().rstrip("/")
        final = (resource.final_url or "").strip().rstrip("/")
        return bool(original and final and original != final)

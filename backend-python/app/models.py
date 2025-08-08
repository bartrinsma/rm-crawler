from dataclasses import dataclass

@dataclass
class SummaryMetrics:
    urls: int = 0
    broken_links: int = 0
    redirects: int = 0
    missing_h1_pct: float = 0.0

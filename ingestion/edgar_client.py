import os
import time
import logging
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# EDGAR base URLs
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"


class EdgarClient:
    """
    Thin wrapper around the SEC EDGAR REST API.

    Responsibilities:
    - Set the required User-Agent header on every request
    - Resolve ticker symbols → CIK numbers
    - Fetch company filing metadata (submissions)
    - Fetch structured financial facts (XBRL)
    - Throttle requests so we don't get rate-limited
    """

    def __init__(self, requests_per_second: float = 2.0):
        self.user_agent = os.getenv("USER_AGENT")
        if not self.user_agent:
            raise ValueError("USER_AGENT must be set in your .env file")

        self.headers = {"User-Agent": self.user_agent}
        self.min_delay = 1.0 / requests_per_second  # seconds between requests
        self._last_request_time = 0.0
        self._ticker_map: dict[str, str] | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _throttle(self):
        """Sleep if needed to stay under the rate limit."""
        elapsed = time.time() - self._last_request_time
        wait = self.min_delay - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_time = time.time()

    def _get(self, url: str) -> dict:
        """Make a throttled GET request. Raises on non-200 status."""
        self._throttle()
        logger.debug(f"GET {url}")
        response = requests.get(url, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def get_ticker_to_cik_map(self) -> dict[str, str]:
        """
        Returns a dict mapping ticker → zero-padded 10-digit CIK string.
        Cached after first fetch to avoid repeated downloads.
        """
        if self._ticker_map is not None:
            return self._ticker_map

        data = self._get(TICKER_MAP_URL)
        self._ticker_map = {
        entry["ticker"]: str(entry["cik_str"]).zfill(10)
        for entry in data.values()
    }
        return self._ticker_map

    def get_cik(self, ticker: str) -> str:
        """Resolve a single ticker to its zero-padded CIK."""
        mapping = self.get_ticker_to_cik_map()
        ticker = ticker.upper()
        if ticker not in mapping:
            raise ValueError(f"Ticker '{ticker}' not found in EDGAR ticker map")
        return mapping[ticker]

    def get_submissions(self, cik: str) -> dict:
        """
        Fetch the submissions history for a company.

        Contains: company name, SIC code, filing history metadata
        (form type, date filed, accession number) for up to 1000 filings.
        Does NOT contain the actual financial numbers — that's companyfacts.
        """
        url = SUBMISSIONS_URL.format(cik=cik)
        return self._get(url)

    def get_company_facts(self, cik: str) -> dict:
        """
        Fetch all XBRL financial facts for a company.

        This is the main financial data endpoint. Returns every reported
        GAAP metric (revenue, assets, net income, etc.) across all periods
        the company has ever filed. This is what we'll use to build our
        time series for anomaly detection.

        Structure:
          facts -> us-gaap -> {metric_name} -> units -> USD -> [{...}]
        Each item in the list is one reported value with:
          - val: the number
          - end: period end date
          - form: which form it came from (10-Q, 10-K)
          - accn: accession number (links back to the actual filing)
        """
        url = COMPANY_FACTS_URL.format(cik=cik)
        return self._get(url)
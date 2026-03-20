"""
currency.py represents the exchange rate management for Pink Tax pipeline

Provides INR and JPY to USD conversion at both:
  - Market rates  (for like-for-like comparison)
  - PPP-adjusted  (World Bank 2024; controls for purchasing power parity)

Update market_rates monthly when running a new scraping round.
Update ppp_rates annually from World Bank ICP data.
"""

from datetime import datetime, date

market_rates = {
    "2024-09-01": {"INR": 1 / 83.9,  "JPY": 1 / 146.5},
    "2024-12-01": {"INR": 1 / 84.4,  "JPY": 1 / 151.8},
    "2025-03-01": {"INR": 1 / 83.5,  "JPY": 1 / 149.0},
}

ppp_rates = {
    "INR": 1 / 24.5,
    "JPY": 1 / 104.0,
}

def _nearest_rate_date(obs_date: str) -> str:
    """
    Find the closest available rate date for a given observation date.
    """

    d = datetime.strptime(obs_date[:10], "%Y-%m-%d").date()
    available = sorted(
        datetime.strptime(k, "%Y-%m-%d").date()
        for k in market_rates
    )

    valid = [r for r in available if r <= d]
    if valid:
        return valid[-1].strftime("%Y-%m-%d")
    
    return available[0].strftime("%Y-%m-%d")


def to_usd(price_local: float, currency: str,
           obs_date: str | None = None) -> float:
    """
    Convert a local price to USD at the market exchange rate.

    Parameters
    ----------
    price_local: price in local currency
    currency: "INR" or "JPY"
    obs_date: ISO date string, uses latest rate if None

    Returns
    -------
    float: price in USD
    """

    if obs_date is None:
        obs_date = str(date.today())
    rate_date = _nearest_rate_date(obs_date)
    rate = market_rates[rate_date][currency]
    return round(price_local * rate, 6)

def to_usd_ppp(price_local: float, currency: str) -> float:
    """
    Convert a local price to PPP-adjusted USD.
    PPP rate is time-invariant (updated annually).
    """

    return round(price_local * ppp_rates[currency], 6)

def get_rate(currency: str, obs_date: str | None = None,
             ppp: bool = False) -> float:
    """
    Get the exchange rate coefficient (usd per local unit).

    Parameters
    ----------
    currency: "INR" or "JPY"
    obs_date: ISO date, uses latest if None
    ppp: if True, returns PPP rate instead of market rate
    """

    if ppp:
        return ppp_rates[currency]
    if obs_date is None:
        obs_date = str(date.today())
    rate_date = _nearest_rate_date(obs_date)
    return market_rates[rate_date][currency]

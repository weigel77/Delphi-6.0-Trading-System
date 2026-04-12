from __future__ import annotations

import unittest
from datetime import date

from services.macro_service import MacroService


TRADING_ECONOMICS_SAMPLE = """
<table>
    <tr data-country="united states" data-category="inflation rate" data-event="inflation rate yoy">
        <td class=" 2026-04-06"><span class="calendar-date-1">08:30 AM</span></td>
        <td class="calendar-item"><table><tr><td><div title="United States" class='flag flag-us'></div></td><td class="calendar-iso">US</td></tr></table></td>
        <td><a class='calendar-event' href='/united-states/inflation-rate'>Inflation Rate YoY</a> <span class="calendar-reference">MAR</span></td>
    </tr>
    <tr data-country="united states" data-category="jobless claims" data-event="initial jobless claims">
        <td class=" 2026-04-02"><span class="calendar-date-1">08:30 AM</span></td>
        <td class="calendar-item"><table><tr><td><div title="United States" class='flag flag-us'></div></td><td class="calendar-iso">US</td></tr></table></td>
        <td><a class='calendar-event' href='/united-states/jobless-claims'>Initial Jobless Claims</a></td>
    </tr>
    <tr data-country="canada" data-category="employment" data-event="employment change">
        <td class=" 2026-04-06"><span class="calendar-date-1">08:30 AM</span></td>
        <td class="calendar-item"><table><tr><td><div title="Canada" class='flag flag-ca'></div></td><td class="calendar-iso">CA</td></tr></table></td>
        <td><a class='calendar-event' href='/canada/employment-change'>Employment Change</a></td>
    </tr>
</table>
"""


MARKETWATCH_SAMPLE = """
<html>
    <body>
        <h3>This Week's Major U.S. Economic Reports & Fed Speakers</h3>
        <table>
            <tr>
                <th>TIME</th>
                <th>Report</th>
                <th>Period</th>
                <th>Actual</th>
                <th>Median Forecast</th>
                <th>Previous</th>
            </tr>
            <tr><td>THURSDAY, APRIL 2</td><td></td><td></td><td></td><td></td><td></td></tr>
            <tr><td>8:30 am</td><td>Initial jobless claims</td><td>Weekly</td><td></td><td></td><td></td></tr>
            <tr><td>10:00 am</td><td>Federal Reserve Chair Jerome Powell speaks</td><td></td><td></td><td></td><td></td></tr>
            <tr><td>MONDAY, APRIL 6</td><td></td><td></td><td></td><td></td><td></td></tr>
            <tr><td>10:00 am</td><td>ISM Services PMI</td><td>Mar.</td><td></td><td></td><td></td></tr>
            <tr><td>1:00 pm</td><td>Federal Reserve Governor Waller speaks</td><td></td><td></td><td></td><td></td></tr>
            <tr><td>3:00 pm</td><td>Consumer Credit</td><td>Feb.</td><td></td><td></td><td></td></tr>
            <tr><td>12:00 pm</td><td>None scheduled</td><td></td><td></td><td></td><td></td></tr>
        </table>
    </body>
</html>
"""


MARKETWATCH_FOMC_MINUTES_SAMPLE = """
<html>
    <body>
        <table>
            <tr>
                <th>TIME</th>
                <th>Report</th>
                <th>Period</th>
                <th>Actual</th>
                <th>Median Forecast</th>
                <th>Previous</th>
            </tr>
            <tr><td>WEDNESDAY, MAY 20</td><td></td><td></td><td></td><td></td><td></td></tr>
            <tr><td>2:00 pm</td><td>Minutes of Fed's May FOMC meeting</td><td></td><td></td><td></td><td></td></tr>
            <tr><td>3:00 pm</td><td>Treasury Auction Results</td><td></td><td></td><td></td><td></td></tr>
        </table>
    </body>
</html>
"""


class MacroServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = MacroService()

    def test_parse_tradingeconomics_html_filters_us_and_classifies_events(self) -> None:
        events = self.service._parse_tradingeconomics_html(
            TRADING_ECONOMICS_SAMPLE,
            checked_dates=[date(2026, 4, 2), date(2026, 4, 6)],
        )

        self.assertIsNotNone(events)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["impact"], "Minor")
        self.assertEqual(events[1]["impact"], "Major")
        self.assertIn("CDT", events[0]["time"])
        self.assertEqual(events[1]["date"], date(2026, 4, 6))

    def test_get_macro_status_uses_fallback_source_when_marketwatch_fails(self) -> None:
        class _FallbackMacroService(MacroService):
            def _load_marketwatch_events(self, checked_dates):
                raise RuntimeError("blocked")

            def _load_tradingeconomics_events(self, checked_dates):
                return {
                    "events": [
                        {
                            "title": "Inflation Rate YoY MAR",
                            "date": date(2026, 4, 6),
                            "time": "7:30 AM CDT",
                            "impact": "Major",
                            "reason": "CPI",
                            "source": "TradingEconomics",
                        }
                    ],
                    "diagnostic": {
                        "response_status": 200,
                        "final_url": "https://tradingeconomics.com/calendar",
                        "parser_strategy": "tradingeconomics-table-rows",
                        "event_count": 1,
                    },
                }

        service = _FallbackMacroService()
        result = service.get_macro_status(date(2026, 4, 6))

        self.assertEqual(result["grade"], "Major")
        self.assertTrue(result["has_major_macro"])
        self.assertTrue(result["fallback_used"])
        self.assertIn("TradingEconomics", result["source_name"])
        self.assertIn("MarketWatch", result["source_name"])
        self.assertEqual(result["macro_events"][0]["title"], "Inflation Rate YoY MAR")

    def test_parse_marketwatch_html_extracts_checked_dates(self) -> None:
        events = self.service._parse_marketwatch_html(
            MARKETWATCH_SAMPLE,
            checked_dates=[date(2026, 4, 2), date(2026, 4, 6)],
        )

        self.assertEqual(len(events), 5)
        self.assertEqual(events[0]["impact"], "Minor")
        self.assertEqual(events[1]["impact"], "Minor")
        self.assertEqual(events[2]["date"], date(2026, 4, 6))
        next_day_events = [event for event in events if event["date"] == date(2026, 4, 6)]
        self.assertEqual(len(next_day_events), 3)
        self.assertEqual({event["impact"] for event in next_day_events}, {"Minor"})
        self.assertIn("Consumer Credit", {event["reason"] for event in next_day_events})
        self.assertIn("Fed Speaker", {event["reason"] for event in next_day_events})
        self.assertIn("CDT", events[0]["time"])

    def test_fomc_minutes_and_other_us_events_default_to_minor(self) -> None:
        events = self.service._parse_marketwatch_html(
            MARKETWATCH_FOMC_MINUTES_SAMPLE,
            checked_dates=[date(2026, 5, 20)],
        )

        self.assertEqual(len(events), 2)
        self.assertEqual([event["impact"] for event in events], ["Minor", "Minor"])
        self.assertEqual(events[0]["reason"], "FOMC Minutes")
        self.assertEqual(events[1]["reason"], "Other US Macro")


if __name__ == "__main__":
    unittest.main()

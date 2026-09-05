from datetime import date, datetime, time, timedelta, timezone

import pytest

from historical import (CoverageEngine, ExpectedBarGenerator, HistoricalIncompleteError,
                        HistoricalProviderPolicy, HistoricalQuality, HistoricalSelector,
                        SelectionMode)

UTC=timezone.utc
START=datetime(2026,9,1,1,0,tzinfo=UTC)
END=datetime(2026,9,1,1,4,tzinfo=UTC)
EXPECTED=tuple(START.replace(minute=minute) for minute in range(4))
POLICIES=(HistoricalProviderPolicy("X",100,HistoricalQuality.BROKER),
          HistoricalProviderPolicy("AKSHARE",50,HistoricalQuality.PUBLIC))


def bar(minute,provider,close=1):
    return {"bar_start":START+timedelta(minutes=minute),"provider":provider,"open":close,
            "high":close,"low":close,"close":close,"volume":None}


def test_two_partial_providers_and_composite_are_deterministic():
    bars=[bar(0,"X"),bar(1,"X"),bar(3,"X"),bar(1,"AKSHARE"),bar(2,"AKSHARE")]
    selector=HistoricalSelector(POLICIES)
    first=selector.select(SelectionMode.COMPOSITE,"SHFE.rb2610","1m",START,END,EXPECTED,bars)
    second=selector.select(SelectionMode.COMPOSITE,"SHFE.rb2610","1m",START,END,EXPECTED,list(reversed(bars)))
    assert first["coverage_ratio"]==1 and first["providers_used"]=={"X":3,"AKSHARE":1}
    assert [(row["bar_start"],row["provider"]) for row in first["bars"]]==[(row["bar_start"],row["provider"]) for row in second["bars"]]
    assert first["providers"][1]["coverage_ratio"]==.75


def test_composite_preserves_gap_and_never_synthesizes():
    result=HistoricalSelector(POLICIES).select(SelectionMode.COMPOSITE,"SHFE.rb2610","1m",START,START.replace(minute=3),EXPECTED[:3],[bar(0,"X"),bar(2,"AKSHARE")])
    assert result["observed_bars"]==2 and result["missing_bars"]==1
    assert [row["bar_start"].minute for row in result["bars"]]==[0,2]


def test_explicit_never_falls_back_and_single_uses_threshold_then_priority():
    bars=[bar(0,"X"),bar(1,"X"),bar(2,"X")]+[bar(i,"AKSHARE") for i in range(4)]
    selector=HistoricalSelector(POLICIES,.95)
    explicit=selector.select(SelectionMode.EXPLICIT,"SHFE.rb2610","1m",START,END,EXPECTED,bars,provider="X")
    assert explicit["selected_provider"]=="X" and len(explicit["bars"])==3
    assert selector.select(SelectionMode.SINGLE,"SHFE.rb2610","1m",START,END,EXPECTED,bars)["selected_provider"]=="AKSHARE"
    high_coverage=[bar(i,"X") for i in range(98)]+[bar(i,"AKSHARE") for i in range(100)]
    expected=tuple(START+timedelta(minutes=i) for i in range(100))
    result=selector.select(SelectionMode.SINGLE,"SHFE.rb2610","1m",expected[0],expected[-1]+timedelta(minutes=1),expected,high_coverage)
    assert result["selected_provider"]=="X"


def test_strict_completeness():
    selector=HistoricalSelector(POLICIES)
    with pytest.raises(HistoricalIncompleteError):
        selector.select(SelectionMode.SINGLE,"SHFE.rb2610","1m",START,END,EXPECTED,[bar(i,"X") for i in range(3)],require_complete=True)
    assert selector.select(SelectionMode.COMPOSITE,"SHFE.rb2610","1m",START,END,EXPECTED,
                           [bar(0,"X"),bar(1,"X"),bar(2,"AKSHARE"),bar(3,"AKSHARE")],require_complete=True)["complete"]


def test_expected_bars_respect_lunch_night_cross_midnight_weekend_and_holiday():
    generator=ExpectedBarGenerator()
    calendar=[{"trading_day":date(2026,9,7),"is_trading_day":True,"night_session_open":date(2026,9,4)},
              {"trading_day":date(2026,9,8),"is_trading_day":False,"night_session_open":None}]
    sessions=[{"start_time":time(21),"end_time":time(1),"crosses_midnight":True},
              {"start_time":time(9),"end_time":time(10),"crosses_midnight":False},
              {"start_time":time(10,30),"end_time":time(11,30),"crosses_midnight":False}]
    values=generator.generate("1h",datetime(2026,9,4,tzinfo=UTC),datetime(2026,9,9,tzinfo=UTC),calendar,sessions)
    local=[value.astimezone(__import__('zoneinfo').ZoneInfo("Asia/Shanghai")) for value in values]
    assert [(value.date(),value.time()) for value in local]==[(date(2026,9,4),time(21)),(date(2026,9,4),time(22)),
        (date(2026,9,4),time(23)),(date(2026,9,5),time(0)),(date(2026,9,7),time(9)),(date(2026,9,7),time(10,30))]


def test_unexpected_closed_session_bar_does_not_increase_coverage():
    coverage=CoverageEngine().calculate("X","SHFE.rb2610","1m",START,END,EXPECTED,[bar(0,"X"),bar(9,"X")])
    assert coverage.observed_bars==1 and coverage.unexpected_bars==1 and coverage.coverage_ratio==.25


def test_akshare_foreign_provider_eligibility_is_interval_aware():
    selector=HistoricalSelector(POLICIES)
    minute=selector.select(SelectionMode.SINGLE,"LME.zn.3m","1m",START,END,EXPECTED,[])
    akshare=next(item for item in minute["providers"] if item["provider"]=="AKSHARE")
    assert akshare["eligible"] is False and akshare["ineligible_reason"]=="INTERVAL_NOT_SUPPORTED"
    daily_expected=(START,)
    daily=selector.select(SelectionMode.SINGLE,"LME.zn.3m","1d",START,START+timedelta(days=1),daily_expected,[])
    akshare=next(item for item in daily["providers"] if item["provider"]=="AKSHARE")
    assert akshare["eligible"] is True and akshare["ineligible_reason"] is None

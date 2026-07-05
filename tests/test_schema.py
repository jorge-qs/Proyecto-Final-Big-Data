"""Tests básicos del contrato de datos."""
from datetime import date
from src.common.schema import Business, Review, User, Checkin, Tip


def test_business_categories_list():
    b = Business(business_id="1", name="Test", city="Lima", state="PE",
                 stars=4.5, review_count=10, categories=["Food", "Mexican"])
    assert isinstance(b.categories, list)
    assert "Food" in b.categories


def test_review_sentiment_optional():
    r = Review(review_id="r1", user_id="u1", business_id="b1",
               stars=5.0, useful=1, funny=0, cool=0,
               text="Excelente lugar", date=date(2023, 6, 1))
    assert r.sentiment is None


def test_user_friends_default_empty():
    u = User(user_id="u1", name="Jorge", review_count=5,
             yelping_since=date(2020, 1, 1), fans=3, average_stars=4.2)
    assert u.friends == []


def test_checkin_fields():
    from datetime import datetime
    c = Checkin(business_id="b1", checkin_ts=datetime(2023, 6, 1, 12, 0))
    assert c.business_id == "b1"


def test_tip_fields():
    t = Tip(text="Buena pizza", date=date(2023, 5, 1),
            compliment_count=2, business_id="b1", user_id="u1")
    assert t.compliment_count == 2

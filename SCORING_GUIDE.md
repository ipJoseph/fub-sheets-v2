# FUB Scoring Configuration Guide

## Overview
All scoring weights are configurable in `.env` file. Adjust these to optimize for your market and sales process.

## Heat Score (Urgency/Engagement)

**What it measures:** How "hot" is this lead right now?

**Current Weights:**
- Website visit: 1.5 points
- Property viewed: 3.0 points  
- Property favorited: 5.0 points ⭐
- Property shared: 1.5 points
- Inbound call: 5.0 points ⭐
- Inbound text: 3.0 points

**Recency Bonuses:**
- 0-3 days: +25 points
- 4-7 days: +15 points
- 8-14 days: +10 points
- 15-30 days: +5 points
- 30+ days: 0 points

**Tuning Tips:**
- Increase `HEAT_WEIGHT_CALL_INBOUND` if phone calls predict closes
- Increase `HEAT_WEIGHT_PROPERTY_FAVORITED` if favorites correlate with deals
- Adjust recency bonuses based on your average sales cycle length

## Priority Score (Overall Ranking)

**Current Formula:**
- Heat: 50% (urgency)
- Value: 20% (commission potential)
- Relationship: 30% (rapport/responsiveness)
- × Stage Multiplier

**Stage Multipliers:**
- Hot Lead: 1.3× (30% boost)
- Active Buyer/Seller: 1.2× (20% boost)
- Nurture: 1.0× (baseline)
- New Lead: 0.9× (10% reduction)
- Cold: 0.7× (30% reduction)

**Market-Specific Tuning:**

Fast/Hot Market:
```bash
PRIORITY_WEIGHT_HEAT=0.60
PRIORITY_WEIGHT_VALUE=0.15
PRIORITY_WEIGHT_RELATIONSHIP=0.25
```

Relationship-Driven:
```bash
PRIORITY_WEIGHT_HEAT=0.35
PRIORITY_WEIGHT_VALUE=0.25
PRIORITY_WEIGHT_RELATIONSHIP=0.40
```

High-End Luxury:
```bash
PRIORITY_WEIGHT_HEAT=0.40
PRIORITY_WEIGHT_VALUE=0.40
PRIORITY_WEIGHT_RELATIONSHIP=0.20
```

## Call List Settings

**Current Thresholds:**
- Minimum Priority: 45
- Maximum Rows: 50

**Adjusting:**
- Too few contacts? Lower `CALL_LIST_MIN_PRIORITY` to 35-40
- Too many contacts? Raise to 50-55
- Need bigger list? Increase `CALL_LIST_MAX_ROWS` to 75-100

## Testing Changes

1. Edit `.env` with new values
2. Run: `python fub_to_sheets_v2.py`
3. Check Google Sheet for score changes
4. Compare call list quality
5. Track which settings correlate with actual closes

## Recommended Testing Approach

**Week 1:** Use defaults, track which leads close
**Week 2:** Increase heat weights by 20%
**Week 3:** Increase value weights by 20%
**Week 4:** Compare results, optimize

Track conversion rates by score ranges to find optimal thresholds.

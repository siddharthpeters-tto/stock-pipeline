# category_config.py

# Order matters. First match wins.

CATEGORY_RULES = [

    # 1️⃣ Structural Growth Leader
    {
        "name": "Structural Growth Leader",
        "growth_min": 0.18,
        "gross_margin_min": 0.50,
        "operating_margin_min": 0.15
    },

    # 2️⃣ Dominant Franchise
    {
        "name": "Dominant Franchise",
        "cap_tier": "Mega Cap",
        "gross_margin_min": 0.45,
        "operating_margin_min": 0.20,
        "max_debt_to_ebitda": 2,
        "min_score": 3
    },

    # 3️⃣ Established Compounder
    {
        "name": "Established Compounder",
        "gross_margin_min": 0.40,
        "operating_margin_min": 0.15,
        "fcf_margin_min": 0.15,
        "max_debt_to_ebitda": 2
    },

    # 4️⃣ Emerging Scaler
    {
        "name": "Emerging Scaler",
        "cap_tier_in": ["Mid Cap", "Small Cap"],
        "growth_min": 0.10,
        "gross_margin_min": 0.35
    }

]

DEFAULT_CATEGORY = "Capital Intensive / Transitional"

def cap_for_score(value, limit):
    return max(min(value, limit), -limit)


def is_saturated(raw_value, capped_value, eps=1e-12):
    return abs(raw_value - capped_value) > eps

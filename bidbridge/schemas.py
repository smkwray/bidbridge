RAW_AUCTIONS_COLUMNS = (
    "auction_date",
    "issue_date",
    "security_type",
    "maturity_bucket",
    "announced_amount",
    "awarded_amount",
    "bid_to_cover",
    "tail_bp",
    "refunding_week",
)

RAW_INVESTOR_CLASS_COLUMNS = (
    "issue_date",
    "security_type",
    "dealer_share",
    "investment_funds_share",
    "foreign_share",
    "depository_share",
    "other_share",
)

RAW_PRIMARY_DEALER_COLUMNS = (
    "week_start",
    "week_end",
    "pd_treasury_inventory",
    "pd_financing_usage",
)

AUCTION_WEEK_PANEL_COLUMNS = (
    "week_start",
    "week_end",
    "auction_count",
    "announced_amount_total",
    "awarded_amount_total",
    "bill_amount",
    "coupon_amount",
    "bill_share",
    "coupon_share",
    "refunding_week",
    "weighted_bid_to_cover",
    "weighted_tail_bp",
    "dealer_share_allotment",
    "investment_funds_share_allotment",
    "foreign_share_allotment",
    "depository_share_allotment",
    "other_share_allotment",
    "nondealer_share",
    "pd_treasury_inventory",
    "pd_financing_usage",
    "inventory_change",
    "dealer_bridge_ratio",
    "financing_intensity",
    "inventory_persistence_proxy",
    "heavy_supply",
    "weak_end_investor_absorption",
    "bridge_episode",
)

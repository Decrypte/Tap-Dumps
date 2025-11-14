#!/usr/bin/env python3
"""
Complete Cohort Analysis - Signup Based
Includes: Overall, Product Split, and Channel Split
Investor sheets: Month, Signups, KYC, KYC%, M0, M1...
AUM sheets: Month, Investors, Current TAI, Active %, M0, M1...
"""

import pymysql
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import warnings

warnings.filterwarnings('ignore')

# ============ CONFIG TO BE ADDED ============


START_MONTH = '2022-01'
END_MONTH = None

DOWNLOADS_FOLDER = os.path.expanduser('~/Downloads')

DEAL_TYPE_PRODUCT_MAP = {
    'ASSET_LEASING': 'Asset Leasing',
    'NON_CONVERTIBLE_DEBENTURES': 'Bonds',
    'INVOICE_DISCOUNTING': 'Invoice Discounting',
    'SECONDARY_NON_CONVERTIBLE_DEBENTURES': 'Bonds',
    'TAP_3M_FIXED': 'P2P',
    'TAP_6M_FIXED': 'P2P',
    'GOLD': 'Gold',
    'FIXED_DEPOSIT': 'Fixed Deposit',
    'PRE_IPO': 'Pre-IPO',
    'SILVER': 'Silver'
}

PRODUCT_ABBR = {
    'Invoice Discounting': 'ID',
    'Asset Leasing': 'AL',
    'Bonds': 'Bonds',
    'Gold': 'Gold',
    'Silver': 'Silver',
    'Pre-IPO': 'Pre-IPO',
    'Fixed Deposit': 'FD',
    'P2P': 'P2P'
}


# ======================================


def get_last_complete_month():
    today = datetime.now()
    last_month = today - relativedelta(months=1)
    return last_month.strftime('%Y-%m')


def fetch_data(start_month, end_month):
    conn = pymysql.connect(**DB_CONFIG)

    try:
        print(f"    Fetching signups with channel attribution...")

        signup_query = f"""
        SELECT 
            p.user_id,
            DATE_FORMAT(p.created_at, '%Y-%m') AS signup_month,
            DATE(p.created_at) AS signup_date,
            CASE
                WHEN iu.utm_source IS NOT NULL THEN 'Paid'
                WHEN iu.utm_source IS NULL
                     AND iu.affiliate_id IN ('JRLADDHA', 'Refer_NC_22', 'THEFINTALES', 
                                            'RANDOMDIMES', 'ANKIT91', 'S9FINTECH', 
                                            'PROSPRR', 'MONEYMANTRA', 'KALPESHPATEL', 
                                            'MUSKAN5000', 'RAM5000', 'TRYLEAF', 
                                            'INVEST100', 'CKLEAF', 'EKLEAF') THEN 'Paid'
                WHEN iu.utm_source IS NULL
                     AND iu.affiliate_id IS NOT NULL
                     AND iu.affiliate_id NOT IN ('JRLADDHA', 'Refer_NC_22', 'THEFINTALES', 
                                                'RANDOMDIMES', 'ANKIT91', 'S9FINTECH', 
                                                'PROSPRR', 'MONEYMANTRA', 'KALPESHPATEL', 
                                                'MUSKAN5000', 'RAM5000', 'TRYLEAF', 
                                                'INVEST100', 'CKLEAF', 'EKLEAF', 'JUNE500') THEN 'Referred'
                WHEN iu.utm_source IS NULL
                     AND (iu.affiliate_id IS NULL OR iu.affiliate_id = 'JUNE500') THEN 'Organic'
                ELSE 'Unknown'
            END AS user_channel
        FROM user.profile p
        LEFT JOIN identity.user iu ON p.user_id = iu.id
        ORDER BY p.created_at
        """

        signups_df = pd.read_sql(signup_query, conn)

        if len(signups_df) == 0:
            return None, None, None

        print(f"    Fetching KYC data...")

        kyc_query = f"""
        SELECT 
            kl.user_id,
            DATE_FORMAT(kl.action_timestamp, '%Y-%m') AS kyc_month
        FROM user.kyc_log kl
        WHERE EXISTS (
            SELECT 1 
            FROM user.profile p 
            WHERE p.user_id = kl.user_id
        )
        AND kl.type = 'BANK_ACCOUNT_CREATE'
        """

        kyc_df = pd.read_sql(kyc_query, conn)

        print(f"    Fetching investments...")

        investment_query = f"""
        SELECT 
            i.user_id,
            DATE_FORMAT(COALESCE(i.date), '%Y-%m') AS invest_month,
            DATE(COALESCE(i.date)) AS invest_date,
            i.amount,
            i.deal_type,
            i.id as investment_id
        FROM deals.investments i
        WHERE EXISTS (
            SELECT 1 
            FROM user.profile p 
            WHERE p.user_id = i.user_id
        )
        AND i.status IN (11, 5)
        AND i.deal_type NOT IN ('POOLING', 'CROSS_SALE')
        ORDER BY invest_month
        """

        investments_df = pd.read_sql(investment_query, conn)

        investments_df['product_type'] = investments_df['deal_type'].map(DEAL_TYPE_PRODUCT_MAP)
        investments_df['product_type'] = investments_df['product_type'].fillna('Other')

        print(f"      -> {len(signups_df)} signups, {len(kyc_df)} KYC records, {len(investments_df)} investments")

        return signups_df, kyc_df, investments_df

    finally:
        conn.close()


def build_cohort_table(signups_df, kyc_df, investments_df, start_month, end_month,
                       metric_type, product_filter=None, channel_filter=None):
    signups_work = signups_df.copy()
    investments_work = investments_df.copy()

    if channel_filter:
        signups_work = signups_work[signups_work['user_channel'] == channel_filter].copy()

    if len(signups_work) == 0:
        return pd.DataFrame()

    if product_filter:
        investments_work = investments_work[investments_work['product_type'] == product_filter].copy()

    if len(investments_work) == 0:
        return pd.DataFrame()

    investments_work['invest_dt'] = pd.to_datetime(investments_work['invest_date'])
    signups_work['signup_dt'] = pd.to_datetime(signups_work['signup_date'])

    investments_work = investments_work.merge(
        signups_work[['user_id', 'signup_month', 'signup_dt']],
        on='user_id',
        how='left'
    )

    start_dt = pd.to_datetime(start_month)
    end_dt = pd.to_datetime(end_month)
    cohort_months = pd.date_range(start=start_dt, end=end_dt, freq='MS')
    current_date = datetime.now()
    last_30_days_start = current_date - timedelta(days=30)

    results = []

    for cohort_date in cohort_months:
        cohort_month = cohort_date.strftime('%Y-%m')

        cohort_signups = signups_work[signups_work['signup_month'] == cohort_month]
        num_signups = len(cohort_signups)

        if num_signups == 0:
            continue

        cohort_users = cohort_signups['user_id'].unique()

        cohort_investments = investments_work[investments_work['user_id'].isin(cohort_users)].copy()

        cohort_investments['months_since'] = (
                (cohort_investments['invest_dt'].dt.year - cohort_date.year) * 12 +
                (cohort_investments['invest_dt'].dt.month - cohort_date.month)
        )

        max_months = (current_date.year - cohort_date.year) * 12 + (current_date.month - cohort_date.month) - 1

        m0_investments = cohort_investments[cohort_investments['months_since'] == 0]
        num_investors = m0_investments['user_id'].nunique()

        if metric_type == 'investors':
            cohort_kyc = kyc_df[
                (kyc_df['user_id'].isin(cohort_users)) &
                (kyc_df['kyc_month'] == cohort_month)
                ]
            num_kyc = cohort_kyc['user_id'].nunique()
            kyc_pct = round((num_kyc / num_signups * 100), 2) if num_signups > 0 else 0.0

        elif metric_type == 'aum':
            recent_investments = cohort_investments[
                cohort_investments['invest_dt'] >= last_30_days_start
                ]
            current_tai = recent_investments['user_id'].nunique()
            active_pct = round((current_tai / num_investors * 100), 2) if num_investors > 0 else None

        m_cols = {}

        for m in range(max_months + 1):
            month_data = cohort_investments[cohort_investments['months_since'] == m]

            if metric_type == 'investors':
                value = month_data['user_id'].nunique()
            elif metric_type == 'aum':
                value = round(month_data['amount'].sum() / 1e7, 2)

            m_cols[f'M{m}'] = value

        if metric_type == 'investors':
            row = {
                'Month': cohort_month,
                'Signups': num_signups,
                'KYC': num_kyc,
                'KYC%': kyc_pct
            }
        elif metric_type == 'aum':
            row = {
                'Month': cohort_month,
                'Investors': num_investors,
                'Current TAI': current_tai,
                'Active %': active_pct
            }

        row.update(m_cols)
        results.append(row)

    df = pd.DataFrame(results)

    if len(df) == 0:
        return df

    m_cols_in_df = [col for col in df.columns if col.startswith('M') and col[1:].isdigit()]
    m_cols_sorted = sorted(m_cols_in_df, key=lambda x: int(x[1:]))

    if metric_type == 'investors':
        base_cols = ['Month', 'Signups', 'KYC', 'KYC%']
    elif metric_type == 'aum':
        base_cols = ['Month', 'Investors', 'Current TAI', 'Active %']

    df = df[base_cols + m_cols_sorted]

    return df


def main():
    print("=" * 70)
    print("COMPLETE COHORT ANALYSIS - SIGNUP BASED")
    print("=" * 70)

    end_month = END_MONTH if END_MONTH else get_last_complete_month()

    print(f"\nConfiguration:")
    print(f"  Date Range: {START_MONTH} to {end_month}")
    print(f"  Cohort Definition: Signup Month")
    print(f"  Analysis: Absolute only (Investors & AUM)")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"cohort_complete_analysis_{timestamp}.xlsx"
    filepath = os.path.join(DOWNLOADS_FOLDER, filename)

    writer = pd.ExcelWriter(filepath, engine='openpyxl')

    print(f"\nFetching data...")
    signups_df, kyc_df, investments_df = fetch_data(START_MONTH, end_month)

    if signups_df is None:
        print("No data found!")
        return

    metrics = [('investors', 'Investors'), ('aum', 'AUM')]

    products = sorted(PRODUCT_ABBR.keys())
    channels = sorted(signups_df['user_channel'].unique().tolist())

    total_sheets = (
            len(metrics) +  # Overall
            len(products) * len(metrics) +  # Product splits
            len(channels) * len(metrics)  # Channel splits
    )

    sheet_count = 0

    print(f"\nGenerating {total_sheets} sheets...\n")

    # ===== OVERALL SHEETS =====
    print("  Processing Overall...")
    for metric_key, metric_name in metrics:
        sheet_count += 1
        sheet_name = f"All Users - {metric_name} - Absolute"

        print(f"    [{sheet_count}/{total_sheets}] {sheet_name}")

        df = build_cohort_table(
            signups_df, kyc_df, investments_df,
            START_MONTH, end_month,
            metric_key
        )

        if len(df) > 0:
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    # ===== PRODUCT SPLIT SHEETS =====
    print("\n  Processing Product Splits...")
    for product in products:
        product_abbr = PRODUCT_ABBR[product]

        for metric_key, metric_name in metrics:
            sheet_count += 1
            sheet_name = f"All Users - {metric_name} - {product_abbr}"

            print(f"    [{sheet_count}/{total_sheets}] {sheet_name}")

            df = build_cohort_table(
                signups_df, kyc_df, investments_df,
                START_MONTH, end_month,
                metric_key,
                product_filter=product
            )

            if len(df) > 0:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    # ===== CHANNEL SPLIT SHEETS =====
    print("\n  Processing Channel Splits...")
    for channel in channels:
        for metric_key, metric_name in metrics:
            sheet_count += 1
            sheet_name = f"All Users - {metric_name} - {channel}"

            print(f"    [{sheet_count}/{total_sheets}] {sheet_name}")

            df = build_cohort_table(
                signups_df, kyc_df, investments_df,
                START_MONTH, end_month,
                metric_key,
                channel_filter=channel
            )

            if len(df) > 0:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    writer.close()

    print(f"\nSuccess! Created {sheet_count} sheets")
    print(f"Saved to: {filepath}")
    print("=" * 70)


if __name__ == '__main__':
    main()
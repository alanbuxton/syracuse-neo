
activity_descriptions = {
    'CorporateFinanceActivity': 'M&A, investments, stock purchases',
    'PartnershipActivity': 'Partnership between 2 or more organizations. Often, companies describe their customers as their partners. This type of activity covers both genuine partnerships and customer/supplier relationships',
    'RoleActivity': 'Key change in senior personnel, e.g. replacing CEO',
    'LocationActivity': 'Opening or closing a new location (e.g. setting up in EMEA or shutting down a factory in a particular town)',
    'ProductActivity': 'New product launches',
    'AnalystRatingActivity': 'Updates from industry analysts',
    'EquityActionsActivity': 'Stock repurchases, dividends etc',
    'FinancialReportingActivity': 'Notice that an organization is going to announce its financials',
    'FinancialsActivity': 'Information about company financials, e.g. revenue or EBITDA',
    'IncidentActivity': 'Adverse incidents e.g. safety',
    'MarketingActivity': 'e.g. launching a new advertising campaign',
    'OperationsActivity': 'Company operations news, e.g. we are investing in a new product, or we have just completed an security audit',
    'RecognitionActivity': 'e.g. we are delighted to announce that we won Agency of the Year',
    'RegulatoryActivity': 'Legal or regulatory activity affecting this organizationg e.g. permit for drilling, or regulatory filing',
}

activity_docstring_raw = ( "An Activity is an event in a corporate lifecycle. We track the following activities\n\n" + 
                          "\n".join( [f" -- {k}: {v}" for k,v in activity_descriptions.items()]) )

activity_docstring_markdown = ("Activity classification label. One of:\n" + 
                               "\n".join( [f" - **{k}**: {v}\n" for k,v in activity_descriptions.items()]))
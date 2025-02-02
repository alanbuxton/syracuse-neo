'''
    Extra model definitions to ensure we don't have errors when trying to resolve node with multiple labels

    To recreate:

from neomodel import db
query = "match (n: Resource) where size(labels(n)) > 2 return distinct labels(n)"
label_sets, _ = db.cypher_query(query)

class_definitions = ""

for labels_arr in label_sets:
    labels = labels_arr[0]
    filtered_labels = [label for label in labels if label != "Resource"]
    if len(filtered_labels) < 2:
        continue
    class_name = "".join(filtered_labels)
    base_classes = ", ".join(filtered_labels) 
    class_definition = f"class {class_name}({base_classes}):\n    __class_name_is_label__ = False\n\n"
    class_definitions = class_definitions + class_definition    
'''

from .models import *

class OrganizationCorporateFinanceActivity(Organization, CorporateFinanceActivity):
    __class_name_is_label__ = False

class OrganizationSite(Organization, Site):
    __class_name_is_label__ = False

class OrganizationPerson(Organization, Person):
    __class_name_is_label__ = False

class OrganizationRole(Organization, Role):
    __class_name_is_label__ = False

class MarketingActivityAboutUsActivity(MarketingActivity, AboutUsActivity):
    __class_name_is_label__ = False

class MarketingActivityOperationsActivity(MarketingActivity, OperationsActivity):
    __class_name_is_label__ = False

class OrganizationMarketingActivity(Organization, MarketingActivity):
    __class_name_is_label__ = False

class OrganizationRegulatoryActivity(Organization, RegulatoryActivity):
    __class_name_is_label__ = False

class OrganizationFinancialsActivity(Organization, FinancialsActivity):
    __class_name_is_label__ = False

class OperationsActivityIncidentActivity(OperationsActivity, IncidentActivity):
    __class_name_is_label__ = False

class OperationsActivityEquityActionsActivity(OperationsActivity, EquityActionsActivity):
    __class_name_is_label__ = False

class OperationsActivityRegulatoryActivity(OperationsActivity, RegulatoryActivity):
    __class_name_is_label__ = False

class OperationsActivityEquityActionsActivityRegulatoryActivity(OperationsActivity, EquityActionsActivity, RegulatoryActivity):
    __class_name_is_label__ = False

class FinancialsActivityFinancialReportingActivity(FinancialsActivity, FinancialReportingActivity):
    __class_name_is_label__ = False

class EquityActionsActivityFinancialsActivity(EquityActionsActivity, FinancialsActivity):
    __class_name_is_label__ = False

class OperationsActivityFinancialsActivity(OperationsActivity, FinancialsActivity):
    __class_name_is_label__ = False

class EquityActionsActivityRegulatoryActivity(EquityActionsActivity, RegulatoryActivity):
    __class_name_is_label__ = False

class MarketingActivityRegulatoryActivity(MarketingActivity, RegulatoryActivity):
    __class_name_is_label__ = False

class OrganizationOperationsActivity(Organization, OperationsActivity):
    __class_name_is_label__ = False

class OperationsActivityAboutUsActivity(OperationsActivity, AboutUsActivity):
    __class_name_is_label__ = False

class OrganizationIncidentActivity(Organization, IncidentActivity):
    __class_name_is_label__ = False

class EquityActionsActivityFinancialReportingActivity(EquityActionsActivity, FinancialReportingActivity):
    __class_name_is_label__ = False

class FinancialsActivityRegulatoryActivity(FinancialsActivity, RegulatoryActivity):
    __class_name_is_label__ = False

class MarketingActivityOperationsActivityRegulatoryActivity(MarketingActivity, OperationsActivity, RegulatoryActivity):
    __class_name_is_label__ = False

class MarketingActivityFinancialReportingActivity(MarketingActivity, FinancialReportingActivity):
    __class_name_is_label__ = False

class OperationsActivityFinancialReportingActivity(OperationsActivity, FinancialReportingActivity):
    __class_name_is_label__ = False

class FinancialsActivityAboutUsActivity(FinancialsActivity, AboutUsActivity):
    __class_name_is_label__ = False

class MarketingActivityEquityActionsActivity(MarketingActivity, EquityActionsActivity):
    __class_name_is_label__ = False

class MarketingActivityFinancialsActivity(MarketingActivity, FinancialsActivity):
    __class_name_is_label__ = False

class MarketingActivityIncidentActivity(MarketingActivity, IncidentActivity):
    __class_name_is_label__ = False

class IncidentActivityFinancialsActivity(IncidentActivity, FinancialsActivity):
    __class_name_is_label__ = False

class OperationsActivityEquityActionsActivityFinancialsActivity(OperationsActivity, EquityActionsActivity, FinancialsActivity):
    __class_name_is_label__ = False

class OperationsActivityAnalystRatingActivity(OperationsActivity, AnalystRatingActivity):
    __class_name_is_label__ = False

class EquityActionsActivityIncidentActivity(EquityActionsActivity, IncidentActivity):
    __class_name_is_label__ = False

class OperationsActivityFinancialsActivityRegulatoryActivity(OperationsActivity, FinancialsActivity, RegulatoryActivity):
    __class_name_is_label__ = False

class OperationsActivityRegulatoryActivityFinancialReportingActivity(OperationsActivity, RegulatoryActivity, FinancialReportingActivity):
    __class_name_is_label__ = False

class EquityActionsActivityFinancialsActivityRegulatoryActivity(EquityActionsActivity, FinancialsActivity, RegulatoryActivity):
    __class_name_is_label__ = False

class MarketingActivityRecognitionActivity(MarketingActivity, RecognitionActivity):
    __class_name_is_label__ = False

class MarketingActivityOperationsActivityEquityActionsActivity(MarketingActivity, OperationsActivity, EquityActionsActivity):
    __class_name_is_label__ = False

class AboutUsActivityFinancialReportingActivity(AboutUsActivity, FinancialReportingActivity):
    __class_name_is_label__ = False

class IncidentActivityRegulatoryActivity(IncidentActivity, RegulatoryActivity):
    __class_name_is_label__ = False

class OrganizationMarketingActivityOperationsActivity(Organization, MarketingActivity, OperationsActivity):
    __class_name_is_label__ = False

class MarketingActivityOperationsActivityFinancialsActivity(MarketingActivity, OperationsActivity, FinancialsActivity):
    __class_name_is_label__ = False

class RegulatoryActivityFinancialReportingActivity(RegulatoryActivity, FinancialReportingActivity):
    __class_name_is_label__ = False

class MarketingActivityOperationsActivityFinancialsActivityRegulatoryActivity(MarketingActivity, OperationsActivity, FinancialsActivity, RegulatoryActivity):
    __class_name_is_label__ = False

class OrganizationEquityActionsActivity(Organization, EquityActionsActivity):
    __class_name_is_label__ = False

class OperationsActivityFinancialsActivityFinancialReportingActivity(OperationsActivity, FinancialsActivity, FinancialReportingActivity):
    __class_name_is_label__ = False

class OrganizationFinancialReportingActivity(Organization, FinancialReportingActivity):
    __class_name_is_label__ = False

class MarketingActivityFinancialReportingActivityRecognitionActivity(MarketingActivity, FinancialReportingActivity, RecognitionActivity):
    __class_name_is_label__ = False

class OperationsActivityIncidentActivityRegulatoryActivity(OperationsActivity, IncidentActivity, RegulatoryActivity):
    __class_name_is_label__ = False

class EquityActionsActivityFinancialsActivityFinancialReportingActivity(EquityActionsActivity, FinancialsActivity, FinancialReportingActivity):
    __class_name_is_label__ = False

class MarketingActivityOperationsActivityFinancialReportingActivity(MarketingActivity, OperationsActivity, FinancialReportingActivity):
    __class_name_is_label__ = False


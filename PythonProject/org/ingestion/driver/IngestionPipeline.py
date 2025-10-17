from org.ingestion.ingestionservice.CharmIndustrial import CharmIndustrial
from org.ingestion.ingestionservice.Allspace import AllSpace
from org.ingestion.ingestionservice.AltusPower import AltusPower
from org.ingestion.ingestionservice.BioBTX import BioBTX
from org.ingestion.ingestionservice.CabanEnergy import CabanEnergy
from org.ingestion.ingestionservice.ChargePoint import ChargePoint
from org.ingestion.ingestionservice.EOCharging import EOCharging
from org.ingestion.ingestionservice.EasyMil import EasyMil
from org.ingestion.ingestionservice.EverstreamAnalytics import EverstreamAnalytics
from org.ingestion.ingestionservice.Prewave import Prewave
from org.ingestion.ingestionservice.SolarKal import SolarKal
from org.ingestion.ingestionservice.H2scan import H2scan


class IngestionPipeline:
    company_id=""
    spark=None

    def __init__(self,company_id):
        self.company_id=company_id

    def execute(self,spark):
        if self.company_id == "5875":
            company_obj = SolarKal()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "11917":
            company_obj = H2scan()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "34005":
            company_obj = EOCharging()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "65212":
            company_obj = Prewave()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "18533":
            company_obj = ChargePoint()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "2805":
            company_obj = EasyMil()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "101741":
            company_obj = EverstreamAnalytics()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "110133":
            company_obj = AltusPower()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "12605":
            company_obj = CharmIndustrial()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "105894":
            company_obj = AllSpace()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "400":
            company_obj = CabanEnergy()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "34204":
            company_obj = BioBTX()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "6134":
            company_obj = ChargePoint()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "12008":
            company_obj = ChargePoint()
            company_obj.execute_ingestion(spark)
        elif self.company_id == "6997":
            company_obj = ChargePoint()
            company_obj.execute_ingestion(spark)
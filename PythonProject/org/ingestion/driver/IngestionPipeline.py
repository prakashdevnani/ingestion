from org.ingestion.solarkal.SolarKal import SolarKal


class IngestionPipeline:
    company_id=""

    def __init__(self,company_id):
        self.company_id=company_id

    def execute(self):
        if self.company_id=="5875":
            company_obj=SolarKal()
            company_obj.execute_ingestion(spark)
        elif self.company_id=="11917":
            company_obj = H2scan()
            company_obj.execute_ingestion(spark)
        elif self.company_id=="34005":
            print("klj")
        elif self.company_id=="65212":
            print("klj")
        elif self.company_id=="18533":
            print("klj")
        elif self.company_id=="2805":
            print("klj")
        elif self.company_id=="101741":
            print("sdf")
        elif self.company_id=="110133":
            print("klj")
        elif self.company_id=="12605":
            print("klj")
        elif self.company_id=="105894":
            print("klj")
        elif self.company_id=="400":
            print("klj")
        elif self.company_id=="34204":
            print("klj")
        elif self.company_id=="6134":
            print("sdf")
        elif self.company_id=="12008":
            print("klj")
        elif self.company_id=="6997":
            print("sdf")
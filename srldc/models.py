from django.db import models
from datetime import date

class Table2AData(models.Model):
    report_date = models.DateField()  # Multiple states per day allowed
    state = models.CharField(max_length=100, null=True, blank=True)

    thermal = models.FloatField(null=True, blank=True)
    hydro = models.FloatField(null=True, blank=True)
    gas_naptha_diesel = models.FloatField(null=True, blank=True)
    solar = models.FloatField(null=True, blank=True)
    wind = models.FloatField(null=True, blank=True)
    others = models.FloatField(null=True, blank=True)
    total = models.FloatField(null=True, blank=True)
    net_sch = models.FloatField(null=True, blank=True)
    drawal = models.FloatField(null=True, blank=True)
    ui = models.FloatField(null=True, blank=True)
    availability = models.FloatField(null=True, blank=True)
    demand_met = models.FloatField(null=True, blank=True)

    shortage = models.FloatField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Table 2A Data for {self.report_date} - {self.state}"

    class Meta:
        verbose_name = "Table 2A Data"
        verbose_name_plural = "Table 2A Data"
        unique_together = ('report_date', 'state')


class Table2CData(models.Model):
    report_date = models.DateField(default=date.today)
    state = models.CharField(max_length=100, null=True, blank=True)

    max_demand_met_of_the_day = models.FloatField(null=True, blank=True)
    time_max_demand_met = models.CharField(max_length=50, null=True, blank=True)
    shortage_during_max_demand = models.FloatField(null=True, blank=True)
    requirement_at_max_demand = models.FloatField(null=True, blank=True)

    max_requirement_of_the_day = models.FloatField(null=True, blank=True)
    time_max_requirement = models.CharField(max_length=50, null=True, blank=True)
    shortage_during_max_requirement = models.FloatField(null=True, blank=True)
    demand_met_at_max_requirement = models.FloatField(null=True, blank=True)

    min_demand_met = models.FloatField(null=True, blank=True)
    time_min_demand_met = models.CharField(max_length=50, null=True, blank=True)

    ace_max = models.FloatField(null=True, blank=True)
    ace_min = models.FloatField(null=True, blank=True)
    time_ace_max = models.CharField(max_length=50, null=True, blank=True)
    time_ace_min = models.CharField(max_length=50, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Table 2C Data for {self.report_date} - {self.state}"

    class Meta:
        verbose_name = "Table 2C Data"
        verbose_name_plural = "Table 2C Data"
        unique_together = ('report_date', 'state')

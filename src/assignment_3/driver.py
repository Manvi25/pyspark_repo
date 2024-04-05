from pyspark_repo.src.assignment_3.utils import *

spark = spark_session()

log_df = create_df(spark, log_data, log_schema)
log_df.show()

log_df_updated = updateColumnName(log_df)
log_df_updated.show()

actions_last_7 = action_performed_last_7(log_df_updated)
actions_last_7.show()

login_date_df = convert_timestamp_login_date(log_df_updated)
login_date_df.show()
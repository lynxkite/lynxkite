import tedx_v1
import tedx_v2
import tedx_v3
import tedx_v4
import lynx.kite

lk = lynx.kite.LynxKite()
input_folder = '/home/petererben/biggraph-dev/remote_api/python/documentation/tedx/automation/input'
# Double dollar is for scala escaping
output_folder = 'DATA$$/tedx/automation/output'

# v1
components_dag = tedx_v1.get_components_wss(lk).to_airflow_DAG('components')

# v2
components_by_date_dag = (tedx_v2.get_components_by_date_wss(lk)
                          .to_airflow_DAG('components_by_date'))
# v3
components_from_inputs_dag = (tedx_v3.get_components_from_inputs_wss(input_folder, lk)
                              .to_airflow_DAG('components_from_inputs'))
# v4
export_results_dag = (tedx_v4.get_export_results_wss(input_folder, output_folder, lk)
                      .to_airflow_DAG('export_results'))

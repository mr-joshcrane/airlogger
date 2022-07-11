package airlogger

import (
	"context"
	"fmt"

	"github.com/apache/airflow-client-go/airflow"
)

func RunCLI() {
	ctx, client := GetClient()
	err, trace := CheckImportErrors(ctx, client)
	if err != nil {
		panic(trace)
	}
	fmt.Println("No import errors detected, continuing")

	err = UnpauseDag(ctx, client)
	if err != nil {
		panic(err)
	}
	fmt.Println("DAG unpaused")
	dr, err := RunDag(ctx, client)
	if err != nil {
		panic(err)
	}

	dagState, err := PollDagRun(ctx, client, dr)
	if err != nil {
		panic(err)
	}
	fmt.Println(dagState)

}

func CheckImportErrors(ctx context.Context, client *airflow.APIClient) (error, string) {
	importErrs, _, err := client.ImportErrorApi.GetImportErrors(ctx).Execute()
	if err != nil {
		return err, ""
	}
	errors := importErrs.GetImportErrors()
	for _, err := range errors {
		fmt.Println(err.GetFilename())
		if err.GetFilename() == "/usr/local/airflow/dags/ozm-ods-borealis.py" {
			stacktrace := err.GetStackTrace()
			return fmt.Errorf("import error for ozm-ods-borealis.py"), stacktrace
		}
	}
	return nil, ""
}

func PollDagRun(ctx context.Context, client *airflow.APIClient, dr airflow.DAGRun) (airflow.DagState, error) {
	var state airflow.DagState
	ID := dr.DagRunId.Get()
	for {
		dr, _, err  := client.DAGRunApi.GetDagRun(ctx, "OZM_ODS_BOREALIS", *ID).Execute()
		state = dr.GetState()
		if err != nil {
			return "", err
		}
		if state == airflow.DAGSTATE_SUCCESS {
			break
		}
		if state == airflow.DAGSTATE_FAILED {
			break
		}
	}
	return state, nil
}

func RunDag(ctx context.Context, client *airflow.APIClient) (airflow.DAGRun, error) {
	dr, _, err  := client.DAGRunApi.PostDagRun(ctx, "OZM_ODS_BOREALIS").Execute()
	if err != nil {
		return airflow.DAGRun{}, err
	}
	return dr, nil
}

func UnpauseDag(ctx context.Context, client *airflow.APIClient) error {
	False := false
	req  := client.DAGApi.PatchDag(ctx, "OZM_ODS_BOREALIS")
	d := airflow.DAG{
		IsPaused: *airflow.NewNullableBool(&False),
	}
	req = req.DAG(d)
	_, _, err := req.Execute()
	return err
}

func GetClient() (context.Context, *airflow.APIClient) {
	conf := airflow.NewConfiguration()
	conf.Host = "localhost:8080"
	conf.Scheme = "http"
	cli := airflow.NewAPIClient(conf)

	cred := airflow.BasicAuth{
		UserName: "admin",
		Password: "admin",
	}
	ctx := context.WithValue(context.Background(), airflow.ContextBasicAuth, cred)
	return ctx, cli

}
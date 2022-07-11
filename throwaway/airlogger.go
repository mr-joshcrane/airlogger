package airlogger

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/airflow-client-go/airflow"
)

func idToFileName(id string) string {
	filename := strings.ToLower(id)
	filename = strings.ReplaceAll(filename, "_", "-")
	filename = filename + ".py"
	return filename
}

func RunCLI(args []string) {
	dagId := strings.ToUpper(args[1])
	filename := idToFileName(dagId)
	fmt.Println(dagId)
	fmt.Println(filename)
	ctx, client := GetClient()
	err, trace := CheckImportErrors(ctx, client, filename)
	if err != nil {
		fmt.Println(err)
		panic(trace)
	}
	fmt.Println("No import errors detected, continuing")

	err = UnpauseDag(ctx, client, dagId)
	if err != nil {
		panic(err)
	}
	fmt.Println("DAG unpaused")
	dr, err := RunDag(ctx, client, dagId)
	if err != nil {
		panic(err)
	}

	dagState, err := PollDagRun(ctx, client, dr)
	if err != nil {
		panic(err)
	}
	fmt.Println(dagState)

}

func CheckImportErrors(ctx context.Context, client *airflow.APIClient, filename string) (error, string) {
	importErrs, _, err := client.ImportErrorApi.GetImportErrors(ctx).Execute()
	if err != nil {
		return err, ""
	}
	errors := importErrs.GetImportErrors()
	for _, err := range errors {
		fmt.Println(err.GetFilename())
		if err.GetFilename() == fmt.Sprintf("/usr/local/airflow/dags/%s", filename) {
			stacktrace := err.GetStackTrace()
			return fmt.Errorf("import error for %s", filename), stacktrace
		}
	}
	return nil, ""
}

func PollDagRun(ctx context.Context, client *airflow.APIClient, dr airflow.DAGRun) (airflow.DagState, error) {
	var state airflow.DagState
	ID := dr.DagRunId.Get()
	for {
		dr, _, err := client.DAGRunApi.GetDagRun(ctx, "OZM_ODS_BOREALIS", *ID).Execute()
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

func RunDag(ctx context.Context, client *airflow.APIClient, dagId string) (airflow.DAGRun, error) {
	dr, _, err := client.DAGRunApi.PostDagRun(ctx, dagId).Execute()
	if err != nil {
		return airflow.DAGRun{}, err
	}
	return dr, nil
}

func UnpauseDag(ctx context.Context, client *airflow.APIClient, dagId string) error {
	False := false
	req := client.DAGApi.PatchDag(ctx, dagId)
	d := airflow.DAG{
		IsPaused: *airflow.NewNullableBool(&False),
	}
	req = req.DAG(d)

	_, resp, err := req.Execute()
	fmt.Println(resp)
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

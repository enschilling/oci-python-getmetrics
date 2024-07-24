import oci
from oci.monitoring.models import ListMetricsDetails, SummarizeMetricsDataDetails
from oci.streaming import StreamClient
from oci.streaming.models import PutMessagesDetails, PutMessagesDetailsEntry
import json
import hashlib

def create_monitoring_client():
    return oci.monitoring.MonitoringClient(oci.config.from_file())

def create_stream_client():
    service_endpoint='https://cell-1.streaming.us-phoenix-1.oci.oraclecloud.com'
    return oci.streaming.StreamClient(oci.config.from_file(), service_endpoint=service_endpoint)


def list_metrics(monitoring_client, namespace, compartment_id):
    list_metrics_details = ListMetricsDetails(
        namespace=namespace
    )
    response = monitoring_client.list_metrics(
        compartment_id=compartment_id,
        list_metrics_details=list_metrics_details
    )
    return response.data

def get_metrics(monitoring_client, namespace, query, compartment_id):
    summarize_metrics_data_details = SummarizeMetricsDataDetails(
        namespace=namespace,
        query=query
    )
    response = monitoring_client.summarize_metrics_data(
        compartment_id=compartment_id,
        summarize_metrics_data_details=summarize_metrics_data_details
    )
    return response.data

def serialize_metric_data(metric_data):
    serialized_data = []
    for metric in metric_data:
        for datapoint in metric.aggregated_datapoints:
            serialized_data.append({
                'name': metric.name,
                'timestamp': datapoint.timestamp.isoformat(),
                'value': datapoint.value,
                'dimensions': metric.dimensions
            })
    return serialized_data

def send_messages_to_stream(client, stream_id, messages):
    messages_to_send = []
    for message in messages:
        value=json.dumps(message)
        key=hashlib.md5(value.encode('utf-8')).hexdigest()
        entry = PutMessagesDetailsEntry(key=key, value=value)
        messages_to_send.append(entry)

    put_message_details = PutMessagesDetails(messages=messages_to_send)
    response = client.put_messages(stream_id, put_message_details)
    return response

def main():
    monitoring_client = create_monitoring_client()
    stream_client = create_stream_client()

    compartment_id = 'ocid1.compartment.oc1..aaaaaaaa67ivd7tzduvd7gowajsrru4kfduaquqqa2f4obs3sc4ex4wf7qza'
    namespace = "oci_computeagent"
    query = "CpuUtilization[1m].mean()"

    # List metrics
    metrics_list = list_metrics(monitoring_client, namespace, compartment_id)
    print("Available Metrics:")
    for metric in metrics_list:
        print(f"Metric name: {metric.name}, Dimensions: {metric.dimensions}")

    # Retrieve and serialize  metrics data
    metrics_data = get_metrics(monitoring_client, namespace, query, compartment_id)
    serialized_data = serialize_metric_data(metrics_data)

    print("Metrics Data: ")
    for data in serialized_data:
        print(data)

    # Stream OCID
    stream_id = 'ocid1.stream.oc1.phx.amaaaaaazy2zpyyaem6apwydjg4yulfpqn645k75ud2jceapz64ji7xn37mq'

    print("================================================================")
    print(serialized_data[0])
    print("============ json dump =========================================")
    print(json.dumps(serialized_data[0]))

#    response = send_messages_to_stream(stream_client, stream_id, serialized_data)
    
#    print("PutMessage response: ", response.data)

if __name__ == "__main__":
    main()

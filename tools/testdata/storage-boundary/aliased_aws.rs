use aws_sdk_s3 as cloud;

async fn bypass(client: &cloud::Client) {
    let _ = client.list_objects_v2();
}

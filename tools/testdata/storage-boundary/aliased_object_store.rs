use object_store as raw_blob;

fn bypass() {
    let _ = raw_blob::aws::AmazonS3Builder::new();
}

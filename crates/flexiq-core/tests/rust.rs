mod rust {
    mod executor_tests;
    // Only compiled with the feature the dispatcher itself lives behind — a
    // default build has no HTTP client to dial the stub with.
    #[cfg(feature = "http-target")]
    mod http_target_tests;
    mod remote_tests;
    mod root_reexport_tests;
    mod storage_tests;
    mod worker_tests;
}

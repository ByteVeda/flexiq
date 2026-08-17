package org.byteveda.flexiq.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.JobFilter;
import org.byteveda.flexiq.model.TaskLogLevel;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Coverage of the native-backed endpoints: logs, replay, job DAG, resources. */
@Timeout(30)
class DashboardNativeTest {

    @SuppressWarnings("unchecked")
    private static Class<Map<String, Object>> mapType() {
        return (Class<Map<String, Object>>) (Class<?>) Map.class;
    }

    private static String seedJob(FlexiQ queue) {
        Task<Map<String, Object>> task = Task.of("email.send", mapType());
        queue.enqueue(
                task,
                Collections.singletonMap("a", 1),
                EnqueueOptions.builder().queue("emails").build());
        return queue.listJobs(JobFilter.builder().limit(1).build()).get(0).id;
    }

    @Test
    void logsAndJobLogs(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                        FlexiQ.builder().sqlite(dir.resolve("t.db").toString()).open();
                DashboardServer server = DashboardServer.start(queue, 0)) {
            String jobId = seedJob(queue);
            queue.writeTaskLog(jobId, "email.send", TaskLogLevel.INFO, "hello-log");
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            assertTrue(client.get("/api/jobs/" + jobId + "/logs").body().contains("hello-log"));
            assertTrue(client.get("/api/logs").body().contains("hello-log"));
        }
    }

    @Test
    void replayAndHistory(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                        FlexiQ.builder().sqlite(dir.resolve("t.db").toString()).open();
                DashboardServer server = DashboardServer.start(queue, 0)) {
            String jobId = seedJob(queue);
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            HttpResponse<String> replay = client.post("/api/jobs/" + jobId + "/replay", null);
            assertEquals(200, replay.statusCode());
            assertTrue(replay.body().contains("replay_job_id"));
            assertTrue(
                    client.get("/api/jobs/" + jobId + "/replay-history").body().contains("replay_job_id"));
        }
    }

    @Test
    void jobDagContainsTheJob(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                        FlexiQ.builder().sqlite(dir.resolve("t.db").toString()).open();
                DashboardServer server = DashboardServer.start(queue, 0)) {
            String jobId = seedJob(queue);
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            String dag = client.get("/api/jobs/" + jobId + "/dag").body();
            assertTrue(dag.contains("\"nodes\"") && dag.contains("\"edges\""));
            assertTrue(dag.contains(jobId));
        }
    }

    @Test
    void resourcesEndpointServed(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                        FlexiQ.builder().sqlite(dir.resolve("t.db").toString()).open();
                DashboardServer server = DashboardServer.start(queue, 0)) {
            DashboardClient client = new DashboardClient(server.port()).as(DashboardClient.seedAdmin(queue));
            // No workers running → an empty resource list.
            assertEquals("[]", client.get("/api/resources").body());
        }
    }
}

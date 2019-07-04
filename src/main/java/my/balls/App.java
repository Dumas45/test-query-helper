package my.balls;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Executor;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class App implements AutoCloseable {
    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException("Invalid argument count! " +
                    "Expected: " +
                    "<users> " +
                    "<tries> " +
                    "<content-type> " +
                    "<accept> " +
                    "<server URI> " +
                    "<path to query body JSON>");
        }

        int users = Integer.parseInt(args[0]);
        int tries = Integer.parseInt(args[1]);
        String contentType = args[2];
        String accept = args[3];
        String serverUri = args[4];
        String contentFile = args[5];

        try (App app = new App(users, tries, contentType, accept, serverUri, contentFile)) {
            app.start();
        }
    }

    private final AtomicInteger ACTIVE_THREADS = new AtomicInteger(0);
    private final AtomicBoolean ERROR_FLAG = new AtomicBoolean(false);
    private final AtomicInteger ERRORS_CNT = new AtomicInteger(0);

    private final int users;
    private final int tries;
    private final String contentType;
    private final String accept;
    private final String serverUri;
    private final String contentFile;

    private final PrintStream timeOut;

    App(int users, int tries, String contentType, String accept, String serverUri, String contentFile) {
        this.users = users;
        this.tries = tries;
        this.contentType = contentType;
        this.accept = accept;
        this.serverUri = serverUri;
        this.contentFile = contentFile;

        PrintStream timeOut;
        Path timingsLogPath = Paths.get("/tmp/query-timings-" + System.currentTimeMillis() + ".txt");
        try {
            timeOut = new PrintStream(new BufferedOutputStream(Files.newOutputStream(timingsLogPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)), true);
            log("Timings log file: " + timingsLogPath);
        } catch (IOException e) {
            log("ERROR: Cannot create timings log file " + timingsLogPath);
            timeOut = null;
        }
        this.timeOut = timeOut;
    }

    @Override
    public void close() {
        if (timeOut != null) {
            timeOut.flush();
            timeOut.close();
        }
    }

    private void start() throws Exception {
        if (users < 1 || users > 100) {
            throw new IllegalArgumentException("users");
        }
        if (tries < 1 || tries > 100) {
            throw new IllegalArgumentException("tries");
        }

        String requestBody = Utils.readFile(contentFile);

        List<Thread> threads = new ArrayList<>();

        for (int i = 1; i <= users; i++) {
            String userName = "user_" + i;
            RestClient restClient = new RestClient(serverUri, userName, userName);

            Thread thread = new Thread(() -> {
                try {
                    for (int Try = 1; ! ERROR_FLAG.get() && Try <= tries; Try++) {
                        String errorMark = "";
                        long time = System.currentTimeMillis();
                        HttpResponse response = restClient.executePostRequest(
                                "rest_v2/queryExecutions",
                                requestBody,
                                accept,
                                contentType);

                        time = System.currentTimeMillis() - time;
                        if (response.getStatusLine().getStatusCode() == 200) {
                            log(userName + "'s  query #" + Try + " completed in " + time/1000 + " seconds");

                            String contentLocation = response.getFirstHeader("Content-Location").getElements()[0].getName();
                            String uriToDelete = contentLocation.replaceAll("/data$", "");

                            response = restClient.executeDeleteRequest(uriToDelete);

                            if (response.getStatusLine().getStatusCode() != 204) {
                                errorMark = "E" + ERRORS_CNT.incrementAndGet() + " ";
                                log("ERROR: EXECUTION DELETION: STATUS: " + response.getStatusLine().getStatusCode());
                            }
                        } else {
                            errorMark = "E" + ERRORS_CNT.incrementAndGet() + " ";
                            log("ERROR: EXECUTION QUERY: STATUS: " + response.getStatusLine().getStatusCode());
                        }

                        logTime(errorMark + time);
                    }
                } catch (Throwable e) {
                    ERROR_FLAG.set(true);
                    e.printStackTrace();
                } finally {
                    ACTIVE_THREADS.decrementAndGet();
                }
            });
            thread.setDaemon(true);
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
            ACTIVE_THREADS.incrementAndGet();
        }

        while (ACTIVE_THREADS.get() > 0 && ! ERROR_FLAG.get()) {
            Thread.sleep(1000);
        }

        Executor.closeIdleConnections();

        if (ERROR_FLAG.get()) {
            System.out.println("FAILED!");
            System.exit(1);
        }
    }

    private void log(String text) {
        System.out.println(text);
    }

    private void logTime(String time) {
        if (timeOut != null) {
            timeOut.println(time);
        }
    }
}

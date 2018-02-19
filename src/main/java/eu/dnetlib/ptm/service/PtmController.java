package eu.dnetlib.ptm.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.madgik.MVTopicModel.PTMFlow;
import org.madgik.dbpediaspotlightclient.DBpediaAnnotator;
import org.madgik.dbpediaspotlightclient.DBpediaAnnotator.AnnotatorType;
import org.madgik.dbpediaspotlightclient.DBpediaAnnotator.ExperimentType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * PTM RESTful service.
 * 
 * @author mhorst
 *
 */
@RestController
public class PtmController implements PtmService {
    

    private static final String PHASE_NAME_ANNOTATE = "annotate";
    
    private static final String PHASE_NAME_TOPIC_MODELING = "topic-modeling";
    
    private static final String PHASE_NAME_EXPORT = "export";
    
    private static final String EXPORT_PARAM_SOURCE_DB = "sourceDB";
    
    private static final String EXPORT_PARAM_TARGET_DB = "targetDB";
    
    private static final String EXPORT_PARAM_TARGET_HOST_NAME = "targetHostName";
    
    private static final String EXPORT_PARAM_EXPERIMENT_ID = "experimentId";

    
    private final Logger log = Logger.getLogger(this.getClass());

    /**
     * Allowing only one thread at a time due to large memory requirements.
     */
    private final ExecutorService fixedSizeExecutor = Executors.newSingleThreadExecutor();
    
    private final Map<String, ExecutionContext> runningTasks = new ConcurrentHashMap<>();
    
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss:SSS");
    
    
    @Value("${retentionDays}")
    private int retentionDays;
    
    /**
     * 
     */
    public PtmController() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
    @RequestMapping("/" + PHASE_NAME_ANNOTATE)
    public String annotate(Command command) {
        
        Map<String,String> properties = command.getMap();
        
        Future<ExecutionResult> future = fixedSizeExecutor.submit(new Callable<ExecutionResult>() {

            @Override
            public ExecutionResult call() throws Exception {

                try {
                    log.debug("starting annotation phase with set of input parameters: " + properties);
                    DBpediaAnnotator c = new DBpediaAnnotator();
                    log.info("DBPedia annotation started");
                    c.getPropValues(properties);
                    log.info("DBPedia annotation: Annotate new publications");
                    c.annotatePubs(ExperimentType.OpenAIRE, AnnotatorType.spotlight);
                    log.info("DBPedia annotation: Get extra fields from DBPedia");
                    c.updateResourceDetails(ExperimentType.OpenAIRE);
                    return new ExecutionResult(JobStatus.succeeded);
                } catch (Exception e) {
                    log.error(e);
                    return new ExecutionResult(e);
                }
            }
            
        });
        
        String jobId = generateNewJobId(PHASE_NAME_ANNOTATE);
        
        runningTasks.put(jobId, new ExecutionContext(properties, future));
        
        return jobId;
    }
    
    @RequestMapping("/" + PHASE_NAME_TOPIC_MODELING)
    public String topicModeling(Command command) {
        
        Map<String,String> properties = command.getMap();
        
        Future<ExecutionResult> future = fixedSizeExecutor.submit(new Callable<ExecutionResult>() {

            @Override
            public ExecutionResult call() throws Exception {
                
                try {
                    log.debug("starting topic-modeling phase with set of input parameters: " + properties);
                    new PTMFlow(properties);
                    return new ExecutionResult(JobStatus.succeeded);
                } catch (Exception e) {
                    log.error(e);
                    return new ExecutionResult(e);
                }
            }

        });
        
        String jobId = generateNewJobId(PHASE_NAME_TOPIC_MODELING);
        
        runningTasks.put(jobId, new ExecutionContext(properties, future));
        
        return jobId;
    }
    
    @RequestMapping("/" + PHASE_NAME_EXPORT)
    public String export(Command command) {
        
        Map<String,String> properties = command.getMap();
        
        Future<ExecutionResult> future = fixedSizeExecutor.submit(new Callable<ExecutionResult>() {

            @Override
            public ExecutionResult call() throws Exception {
                
                try {
                    log.debug("starting export phase with set of input parameters: " + properties);
                    
                    // 1st) copying classpath resource to tmpdir in order to make it available from commandline
                    String scriptFileName = "export.sh";
                    String scriptLocation = System.getProperty("java.io.tmpdir") + File.separatorChar + scriptFileName;
                    ClassPathResource cp = new ClassPathResource("scripts/" + scriptFileName);
                    try (InputStream inStream = cp.getInputStream();
                            OutputStream outStream = new FileOutputStream(scriptLocation)) {
                        IOUtils.copy(inStream, outStream);
                    }
                    
                    // 2nd) triggering script execution
                    String stdErr = null;
                    Process process = new ProcessBuilder(buildExportProcessCommands(scriptLocation, properties)).start();
                    try {
                        process.waitFor();
                        try (InputStream inputStream = process.getInputStream()) {
                            String stdOut = IOUtils.toString(inputStream, "utf8");
                            if (StringUtils.isNotBlank(stdOut)) {
                                log.info("export shell script output: " + stdOut);
                            }
                        }
                        try (InputStream errStream = process.getErrorStream()) {
                            stdErr = IOUtils.toString(errStream, "utf8");
                            if (StringUtils.isNotBlank(stdErr)) {
                                log.error("export shell script stderr: " + stdErr);    
                            }
                        }
                    } catch (Exception e) {
                        throw new IOException("got error while running export script: " + getErrorMessage(process.getErrorStream()), e);
                    }
                    
                    if (process.exitValue() != 0) {
                        throw new RuntimeException("export script execution failed with exit value: "
                                + process.exitValue() + " and error: " + stdErr);
                    } else {
                        return new ExecutionResult(JobStatus.succeeded);    
                    }
                    
                } catch (Exception e) {
                    log.error(e);
                    return new ExecutionResult(e);
                }
            }
        });
        
        String jobId = generateNewJobId(PHASE_NAME_EXPORT);
        
        runningTasks.put(jobId, new ExecutionContext(properties, future));
        
        return jobId;
    }
    
    @RequestMapping("/report/{jobId}")
    public ExecutionReport getReport(@PathVariable(value="jobId") String jobId) throws PtmException {
        ExecutionContext executionContext = runningTasks.get(jobId);
        if (executionContext != null) {
            if (executionContext.getFuture().isCancelled()) {
                return new ExecutionReport(executionContext.getParameters(), 
                        JobStatus.interrupted, null);
            } else if (executionContext.getFuture().isDone()) {
                try {
                    ExecutionResult result = executionContext.getFuture().get();
                    return new ExecutionReport(executionContext.getParameters(), 
                            result.getJobStatus(), result.getErrorDetails());
                } catch (Exception e) {
                    throw new PtmException(e);
                }
            } else {
                return new ExecutionReport(executionContext.getParameters(), 
                        JobStatus.ongoing, null);
            }
        } else {
            throw new PtmException("invalid jobId: " + jobId);
        }
    }
    
    @RequestMapping("/list")
    public Set<String> listJobs() {
        return new TreeSet<String>(runningTasks.keySet());
    }

    @RequestMapping("/cancel/{jobId}")
    public boolean cancel(@PathVariable("jobId") String jobId) throws PtmException {
        ExecutionContext executionContext = runningTasks.get(jobId);
        if (executionContext != null) {
            boolean result = executionContext.getFuture().cancel(true);
            log.info("job: '" + jobId + "' was canceled with result: " + result);
            return result;
        } else {
            throw new PtmException("invalid jobId: " + jobId);
        }
    }
    
    @Scheduled(cron = "${cleanupCronSchedule}")
    public void cleanup() {
        log.debug("performing cleanup operation for results canceled or older than " + retentionDays + " days");
        long retentionMargin = System.currentTimeMillis() - (86400000l * retentionDays);
        runningTasks.entrySet().removeIf(e -> shouldBeRemoved(e.getValue(), retentionMargin));
    }
    
    // ------------------------ PRIVATE -----------------------------
    
    private static List<String> buildExportProcessCommands(String scriptLocation, Map<String,String> properties) {
        List<String> commands = new ArrayList<>();
        commands.add("bash");
        commands.add(scriptLocation);
        // list of parameters below has to be aligned with script input parameters
        addCommandParameter(properties, EXPORT_PARAM_SOURCE_DB, commands);
        addCommandParameter(properties, EXPORT_PARAM_TARGET_DB, commands);
        addCommandParameter(properties, EXPORT_PARAM_TARGET_HOST_NAME, commands);
        addCommandParameter(properties, EXPORT_PARAM_EXPERIMENT_ID, commands);
        return commands;
    }
    
    private static void addCommandParameter(Map<String,String> properties, String paramName, List<String> commands) {
        if (properties != null && properties.containsKey(paramName)) {
            commands.add(properties.get(paramName));
        } else {
            throw new IllegalArgumentException("input parameter missing: " + paramName);
        }
    }
    
    private static boolean shouldBeRemoved(ExecutionContext context, long retentionMargin) {
        try {
            return (context.getFuture().isCancelled() ||
                    (context.getFuture().isDone() && context.getFuture().get().getCreationTimestamp() < retentionMargin));
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * @return new job identifier
     */
    private String generateNewJobId(String phaseName) {
        StringBuilder strBuilder = new StringBuilder(dateFormat.format(new Date()));
        strBuilder.append('-');
        strBuilder.append(phaseName);
        strBuilder.append('-');
        strBuilder.append(UUID.randomUUID().toString());
        return strBuilder.toString();
    }
    
    /**
     * Provides error message from error stream.
     */
    private static String getErrorMessage(InputStream errorStream) throws UnsupportedEncodingException, IOException {
        StringBuilder errorBuilder = new StringBuilder();
        try (BufferedReader stderr = new BufferedReader(new InputStreamReader(errorStream, "utf8"))) {
            String line;
            while ((line = stderr.readLine()) != null) {
                errorBuilder.append(line);
            }
        }
        return errorBuilder.toString();
    }
    
    // --------------------------- INNER CLASS -----------------------
    
    /**
     * Result of asynchronous computation.
     * Error details are set only when error occurs.
     * @author mhorst
     *
     */
    public class ExecutionResult {
        
        private final JobStatus jobStatus;
        
        private final ErrorDetails errorDetails;
        
        private final long creationTimestamp;
        
        public ExecutionResult(JobStatus jobStatus, ErrorDetails errorDetails) {
            this.jobStatus = jobStatus;
            this.errorDetails = errorDetails;
            this.creationTimestamp = System.currentTimeMillis();
        }

        public ExecutionResult(JobStatus jobStatus) {
            this(jobStatus, null);
        }
        
        public ExecutionResult(Throwable e) {
            this(e instanceof InterruptedException? JobStatus.interrupted : JobStatus.failed, new ErrorDetails(e));
        }

        public JobStatus getJobStatus() {
            return jobStatus;
        }

        public ErrorDetails getErrorDetails() {
            return errorDetails;
        }

        public long getCreationTimestamp() {
            return creationTimestamp;
        }
    }
    
    /**
     * Execution context encapsulating input parameters and reference to result of asynchronous computation.
     *
     */
    public class ExecutionContext {
        
        private final Map<String, String> parameters;
        
        private final Future<ExecutionResult> future;
        
        public ExecutionContext(Map<String, String> parameters, Future<ExecutionResult> future) {
            this.parameters = parameters;
            this.future = future;
        }

        public Map<String, String> getParameters() {
            return parameters;
        }

        public Future<ExecutionResult> getFuture() {
            return future;
        }
    }

}

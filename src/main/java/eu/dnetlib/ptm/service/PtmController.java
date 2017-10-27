package eu.dnetlib.ptm.service;

import java.text.SimpleDateFormat;
import java.util.Date;
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

import org.apache.log4j.Logger;
import org.madgik.MVTopicModel.PTMFlow;
import org.madgik.dbpediaspotlightclient.DBpediaAnnotator;
import org.madgik.dbpediaspotlightclient.DBpediaAnnotator.AnnotatorType;
import org.madgik.dbpediaspotlightclient.DBpediaAnnotator.ExperimentType;
import org.springframework.beans.factory.annotation.Value;
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

package flume.source;

import flume.source.BriteCloudReliableSpoolingFileEventReader.Builder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.ConsumeOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BriteCloudSpoolDirectorySource extends AbstractSource implements Configurable, EventDrivenSource {
	private static final Logger logger = LoggerFactory.getLogger(BriteCloudSpoolDirectorySource.class);
	private static final int POLL_DELAY_MS = 500;
	private String completedSuffix;
	private String spoolDirectory;
	private boolean fileHeader;
	private String fileHeaderKey;
	private boolean basenameHeader;
	private String basenameHeaderKey;
	private int batchSize;
	private String ignorePattern;
	private String trackerDirPath;
	private String deserializerType;
	private Context deserializerContext;
	private String deletePolicy;
	private String inputCharset;
	private int undoKeepTime;
	private DecodeErrorPolicy decodeErrorPolicy;
	private volatile boolean hasFatalError = false;
	private SourceCounter sourceCounter;
	BriteCloudReliableSpoolingFileEventReader reader;
	private ScheduledExecutorService executor;
	private boolean backoff = true;
	private boolean hitChannelException = false;
	private int maxBackoff;
	private ConsumeOrder consumeOrder;

	public BriteCloudSpoolDirectorySource() {
	}

	public synchronized void start() {
		logger.info("SpoolDirectorySource source starting with directory: {}", this.spoolDirectory);
		this.executor = Executors.newSingleThreadScheduledExecutor();
		File directory = new File(this.spoolDirectory);

		try {
			this.reader = (new Builder()).spoolDirectory(directory).completedSuffix(this.completedSuffix).ignorePattern(this.ignorePattern).trackerDirPath(this.trackerDirPath).annotateFileName(this.fileHeader).fileNameHeader(this.fileHeaderKey).annotateBaseName(this.basenameHeader).baseNameHeader(this.basenameHeaderKey).deserializerType(this.deserializerType).deserializerContext(this.deserializerContext).deletePolicy(this.deletePolicy).inputCharset(this.inputCharset).decodeErrorPolicy(this.decodeErrorPolicy).consumeOrder(this.consumeOrder).undoKeepTime(this.undoKeepTime).build();
		} catch (IOException var3) {
			throw new FlumeException("Error instantiating spooling event parser", var3);
		}

		Runnable runner = new BriteCloudSpoolDirectorySource.SpoolDirectoryRunnable(this.reader, this.sourceCounter);
		this.executor.scheduleWithFixedDelay(runner, 0L, 500L, TimeUnit.MILLISECONDS);
		super.start();
		logger.debug("SpoolDirectorySource source started");
		this.sourceCounter.start();
	}

	public synchronized void stop() {
		this.executor.shutdown();

		try {
			this.executor.awaitTermination(10L, TimeUnit.SECONDS);
		} catch (InterruptedException var2) {
			logger.info("Interrupted while awaiting termination", var2);
		}

		this.executor.shutdownNow();
		super.stop();
		this.sourceCounter.stop();
		logger.info("SpoolDir source {} stopped. Metrics: {}", this.getName(), this.sourceCounter);
	}

	public String toString() {
		return "Spool Directory source " + this.getName() + ": { spoolDir: " + this.spoolDirectory + " }";
	}

	public synchronized void configure(Context context) {
		this.spoolDirectory = context.getString("spoolDir");
		Preconditions.checkState(this.spoolDirectory != null, "Configuration must specify a spooling directory");
		this.completedSuffix = context.getString("fileSuffix", ".COMPLETED");
		this.deletePolicy = context.getString("deletePolicy", "never");
		this.fileHeader = context.getBoolean("fileHeader", false).booleanValue();
		this.fileHeaderKey = context.getString("fileHeaderKey", "file");
		this.basenameHeader = context.getBoolean("basenameHeader", false).booleanValue();
		this.basenameHeaderKey = context.getString("basenameHeaderKey", "basename");
		this.batchSize = context.getInteger("batchSize", Integer.valueOf(100)).intValue();
		this.inputCharset = context.getString("inputCharset", "UTF-8");
		this.decodeErrorPolicy = DecodeErrorPolicy.valueOf(context.getString("decodeErrorPolicy", SpoolDirectorySourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY).toUpperCase(Locale.ENGLISH));
		this.ignorePattern = context.getString("ignorePattern", "^$");
		this.trackerDirPath = context.getString("trackerDir", ".flumespool");
		this.deserializerType = context.getString("deserializer", "LINE");
		this.deserializerContext = new Context(context.getSubProperties("deserializer."));
		this.consumeOrder = ConsumeOrder.valueOf(context.getString("consumeOrder", SpoolDirectorySourceConfigurationConstants.DEFAULT_CONSUME_ORDER.toString()).toUpperCase(Locale.ENGLISH));
		this.undoKeepTime = context.getInteger("undoKeepTime", Integer.valueOf(0)).intValue();
		Integer bufferMaxLineLength = context.getInteger("bufferMaxLineLength");
		if (bufferMaxLineLength != null && this.deserializerType != null && this.deserializerType.equalsIgnoreCase("LINE")) {
			this.deserializerContext.put("maxLineLength", bufferMaxLineLength.toString());
		}

		this.maxBackoff = context.getInteger("maxBackoff", SpoolDirectorySourceConfigurationConstants.DEFAULT_MAX_BACKOFF).intValue();
		if (this.sourceCounter == null) {
			this.sourceCounter = new SourceCounter(this.getName());
		}

	}

	@VisibleForTesting
	protected boolean hasFatalError() {
		return this.hasFatalError;
	}

	@VisibleForTesting
	protected void setBackOff(boolean backoff) {
		this.backoff = backoff;
	}

	@VisibleForTesting
	protected boolean hitChannelException() {
		return this.hitChannelException;
	}

	@VisibleForTesting
	protected SourceCounter getSourceCounter() {
		return this.sourceCounter;
	}

	private class SpoolDirectoryRunnable implements Runnable {
		private BriteCloudReliableSpoolingFileEventReader reader;
		private SourceCounter sourceCounter;

		public SpoolDirectoryRunnable(BriteCloudReliableSpoolingFileEventReader reader, SourceCounter sourceCounter) {
			this.reader = reader;
			this.sourceCounter = sourceCounter;
		}

		public void run() {
			int backoffInterval = 250;

			try {
				while(!Thread.interrupted()) {
					List<Event> events = this.reader.readEvents(BriteCloudSpoolDirectorySource.this.batchSize);
					if (events.isEmpty()) {
						break;
					}

					this.sourceCounter.addToEventReceivedCount((long)events.size());
					this.sourceCounter.incrementAppendBatchReceivedCount();

					try {
						BriteCloudSpoolDirectorySource.this.getChannelProcessor().processEventBatch(events);
						this.reader.commit();
					} catch (ChannelException var4) {
						BriteCloudSpoolDirectorySource.logger.warn("The channel is full, and cannot write data now. The source will try again after " + String.valueOf(backoffInterval) + " milliseconds");
						BriteCloudSpoolDirectorySource.this.hitChannelException = true;
						if (BriteCloudSpoolDirectorySource.this.backoff) {
							TimeUnit.MILLISECONDS.sleep((long)backoffInterval);
							backoffInterval <<= 1;
							backoffInterval = backoffInterval >= BriteCloudSpoolDirectorySource.this.maxBackoff ? BriteCloudSpoolDirectorySource.this.maxBackoff : backoffInterval;
						}
						continue;
					}

					backoffInterval = 250;
					this.sourceCounter.addToEventAcceptedCount((long)events.size());
					this.sourceCounter.incrementAppendBatchAcceptedCount();
				}
			} catch (Throwable var5) {
				BriteCloudSpoolDirectorySource.logger.error("FATAL: " + BriteCloudSpoolDirectorySource.this.toString() + ": Uncaught exception in SpoolDirectorySource thread. Restart or reconfigure Flume to continue processing.", var5);
				BriteCloudSpoolDirectorySource.this.hasFatalError = true;
				Throwables.propagate(var5);
			}

		}
	}
}

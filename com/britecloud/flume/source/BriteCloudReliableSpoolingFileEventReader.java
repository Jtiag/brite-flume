package flume.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience.Private;
import org.apache.flume.annotations.InterfaceStability.Evolving;
import org.apache.flume.annotations.InterfaceStability.Unstable;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.serialization.DurablePositionTracker;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.EventDeserializerFactory;
import org.apache.flume.serialization.PositionTracker;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.apache.flume.serialization.ResettableInputStream;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.ConsumeOrder;
import org.apache.flume.tools.PlatformDetect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Administrator
 */
@Private
@Evolving
public class BriteCloudReliableSpoolingFileEventReader implements ReliableEventReader {
    private static final Logger logger = LoggerFactory.getLogger(BriteCloudReliableSpoolingFileEventReader.class);
    static final String META_FILE_NAME = ".flumespool-main.meta";
    private final File spoolDirectory;
    private final String completedSuffix;
    private final String deserializerType;
    private final Context deserializerContext;
    private final Pattern ignorePattern;
    private final File metaFile;
    private final boolean annotateFileName;
    private final boolean annotateBaseName;
    private final String fileNameHeader;
    private final String baseNameHeader;
    private final String deletePolicy;
    private final Charset inputCharset;
    private final DecodeErrorPolicy decodeErrorPolicy;
    private final ConsumeOrder consumeOrder;
    private final int undoKeepTime;
    private Optional<BriteCloudReliableSpoolingFileEventReader.FileInfo> currentFile;
    private Optional<BriteCloudReliableSpoolingFileEventReader.FileInfo> lastFileRead;
    private boolean committed;
    private Iterator<File> candidateFileIter;
    private int listFilesCount;

    private BriteCloudReliableSpoolingFileEventReader(File spoolDirectory, String completedSuffix, String ignorePattern, String trackerDirPath, boolean annotateFileName, String fileNameHeader, boolean annotateBaseName, String baseNameHeader, String deserializerType, Context deserializerContext, String deletePolicy, String inputCharset, DecodeErrorPolicy decodeErrorPolicy, ConsumeOrder consumeOrder, int undoKeepTime, Object o) throws IOException {
        this.currentFile = Optional.absent();
        this.lastFileRead = Optional.absent();
        this.committed = true;
        this.candidateFileIter = null;
        this.listFilesCount = 0;
        Preconditions.checkNotNull(spoolDirectory);
        Preconditions.checkNotNull(completedSuffix);
        Preconditions.checkNotNull(ignorePattern);
        Preconditions.checkNotNull(trackerDirPath);
        Preconditions.checkNotNull(deserializerType);
        Preconditions.checkNotNull(deserializerContext);
        Preconditions.checkNotNull(deletePolicy);
        Preconditions.checkNotNull(inputCharset);
        if (!deletePolicy.equalsIgnoreCase(BriteCloudReliableSpoolingFileEventReader.DeletePolicy.NEVER.name()) && !deletePolicy.equalsIgnoreCase(BriteCloudReliableSpoolingFileEventReader.DeletePolicy.IMMEDIATE.name())) {
            throw new IllegalArgumentException("Delete policies other than NEVER and IMMEDIATE are not yet supported");
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Initializing {} with directory={}, metaDir={}, deserializer={}", new Object[]{BriteCloudReliableSpoolingFileEventReader.class.getSimpleName(), spoolDirectory, trackerDirPath, deserializerType});
            }

            Preconditions.checkState(spoolDirectory.exists(), "Directory does not exist: " + spoolDirectory.getAbsolutePath());
            Preconditions.checkState(spoolDirectory.isDirectory(), "Path is not a directory: " + spoolDirectory.getAbsolutePath());

            File trackerDirectory;
            try {
                trackerDirectory = File.createTempFile("flume-spooldir-perm-check-", ".canary", spoolDirectory);
                Files.write("testing flume file permissions\n", trackerDirectory, Charsets.UTF_8);
                List<String> lines = Files.readLines(trackerDirectory, Charsets.UTF_8);
                Preconditions.checkState(!lines.isEmpty(), "Empty canary file %s", new Object[]{trackerDirectory});
                if (!trackerDirectory.delete()) {
                    throw new IOException("Unable to delete canary file " + trackerDirectory);
                }

                logger.debug("Successfully created and deleted canary file: {}", trackerDirectory);
            } catch (IOException var18) {
                throw new FlumeException("Unable to read and modify files in the spooling directory: " + spoolDirectory, var18);
            }

            if (undoKeepTime < 0) {
                undoKeepTime = 0;
            }

            this.spoolDirectory = spoolDirectory;
            this.completedSuffix = completedSuffix;
            this.deserializerType = deserializerType;
            this.deserializerContext = deserializerContext;
            this.annotateFileName = annotateFileName;
            this.fileNameHeader = fileNameHeader;
            this.annotateBaseName = annotateBaseName;
            this.baseNameHeader = baseNameHeader;
            this.ignorePattern = Pattern.compile(ignorePattern);
            this.deletePolicy = deletePolicy;
            this.inputCharset = Charset.forName(inputCharset);
            this.decodeErrorPolicy = (DecodeErrorPolicy) Preconditions.checkNotNull(decodeErrorPolicy);
            this.consumeOrder = (ConsumeOrder) Preconditions.checkNotNull(consumeOrder);
            this.undoKeepTime = undoKeepTime;
            trackerDirectory = new File(trackerDirPath);
            if (!trackerDirectory.isAbsolute()) {
                trackerDirectory = new File(spoolDirectory, trackerDirPath);
            }

            if (!trackerDirectory.exists() && !trackerDirectory.mkdir()) {
                throw new IOException("Unable to mkdir nonexistent meta directory " + trackerDirectory);
            } else if (!trackerDirectory.isDirectory()) {
                throw new IOException("Specified meta directory is not a directory" + trackerDirectory);
            } else {
                this.metaFile = new File(trackerDirectory, ".flumespool-main.meta");
                if (this.metaFile.exists() && this.metaFile.length() == 0L) {
                    this.deleteMetaFile();
                }

            }
        }
    }

    @VisibleForTesting
    int getListFilesCount() {
        return this.listFilesCount;
    }

    public String getLastFileRead() {
        return !this.lastFileRead.isPresent() ? null : ((BriteCloudReliableSpoolingFileEventReader.FileInfo) this.lastFileRead.get()).getFile().getAbsolutePath();
    }

    @Override
    public Event readEvent() throws IOException {
        List<Event> events = this.readEvents(1);
        return !events.isEmpty() ? (Event) events.get(0) : null;
    }

    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        if (!this.committed) {
            if (!this.currentFile.isPresent()) {
                throw new IllegalStateException("File should not roll when commit is outstanding.");
            }

            logger.info("Last read was never committed - resetting mark position.");
            ((BriteCloudReliableSpoolingFileEventReader.FileInfo) this.currentFile.get()).getDeserializer().reset();
        } else {
            if (!this.currentFile.isPresent()) {
                this.currentFile = this.getNextFile();
            }

            if (!this.currentFile.isPresent()) {
                return Collections.emptyList();
            }
        }

        EventDeserializer des = ((BriteCloudReliableSpoolingFileEventReader.FileInfo) this.currentFile.get()).getDeserializer();
        List events = Collections.emptyList();

        try {
            events = des.readEvents(numEvents);
        } catch (Exception var8) {
            var8.printStackTrace();
        }

        while (events.isEmpty()) {
            logger.info("Last read took us just up to a file boundary. Rolling to the next file, if there is one.");
            this.retireCurrentFile();
            this.currentFile = this.getNextFile();
            if (!this.currentFile.isPresent()) {
                return Collections.emptyList();
            }

            try {
                events = ((BriteCloudReliableSpoolingFileEventReader.FileInfo) this.currentFile.get()).getDeserializer().readEvents(numEvents);
            } catch (Exception var7) {
                var7.printStackTrace();
            }
        }

        String basename;
        Iterator var5;
        Event event;
        if (this.annotateFileName) {
            basename = ((BriteCloudReliableSpoolingFileEventReader.FileInfo) this.currentFile.get()).getFile().getAbsolutePath();
            var5 = events.iterator();

            while (var5.hasNext()) {
                event = (Event) var5.next();
                event.getHeaders().put(this.fileNameHeader, basename);
            }
        }

        if (this.annotateBaseName) {
            basename = ((BriteCloudReliableSpoolingFileEventReader.FileInfo) this.currentFile.get()).getFile().getName();
            var5 = events.iterator();

            while (var5.hasNext()) {
                event = (Event) var5.next();
                event.getHeaders().put(this.baseNameHeader, basename);
            }
        }

        this.committed = false;
        this.lastFileRead = this.currentFile;
        return events;
    }

    @Override
    public void close() throws IOException {
        if (this.currentFile.isPresent()) {
            ((BriteCloudReliableSpoolingFileEventReader.FileInfo) this.currentFile.get()).getDeserializer().close();
            this.currentFile = Optional.absent();
        }

    }

    @Override
    public void commit() throws IOException {
        if (!this.committed && this.currentFile.isPresent()) {
            ((BriteCloudReliableSpoolingFileEventReader.FileInfo) this.currentFile.get()).getDeserializer().mark();
            this.committed = true;
        }

    }

    private void retireCurrentFile() throws IOException {
        Preconditions.checkState(this.currentFile.isPresent());
        File fileToRoll = new File(((BriteCloudReliableSpoolingFileEventReader.FileInfo) this.currentFile.get()).getFile().getAbsolutePath());
        ((BriteCloudReliableSpoolingFileEventReader.FileInfo) this.currentFile.get()).getDeserializer().close();
        String message;
        if (fileToRoll.lastModified() != ((BriteCloudReliableSpoolingFileEventReader.FileInfo) this.currentFile.get()).getLastModified()) {
            message = "File has been modified since being read: " + fileToRoll;
            throw new IllegalStateException(message);
        } else if (fileToRoll.length() != ((BriteCloudReliableSpoolingFileEventReader.FileInfo) this.currentFile.get()).getLength()) {
            message = "File has changed size since being read: " + fileToRoll;
            throw new IllegalStateException(message);
        } else {
            if (this.deletePolicy.equalsIgnoreCase(BriteCloudReliableSpoolingFileEventReader.DeletePolicy.NEVER.name())) {
                this.rollCurrentFile(fileToRoll);
            } else {
                if (!this.deletePolicy.equalsIgnoreCase(BriteCloudReliableSpoolingFileEventReader.DeletePolicy.IMMEDIATE.name())) {
                    throw new IllegalArgumentException("Unsupported delete policy: " + this.deletePolicy);
                }

                this.deleteCurrentFile(fileToRoll);
            }

        }
    }

    private void rollCurrentFile(File fileToRoll) throws IOException {
        File dest = new File(fileToRoll.getPath() + this.completedSuffix);
        logger.info("Preparing to move file {} to {}", fileToRoll, dest);
        boolean renamed;
        String message;
        if (dest.exists() && PlatformDetect.isWindows()) {
            if (!Files.equal(((BriteCloudReliableSpoolingFileEventReader.FileInfo) this.currentFile.get()).getFile(), dest)) {
                message = "File name has been re-used with different files. Spooling assumptions violated for " + dest;
                throw new IllegalStateException(message);
            }

            logger.warn("Completed file " + dest + " already exists, but files match, so continuing.");
            renamed = fileToRoll.delete();
            if (!renamed) {
                logger.error("Unable to delete file " + fileToRoll.getAbsolutePath() + ". It will likely be ingested another time.");
            }
        } else {
            if (dest.exists()) {
                message = "File name has been re-used with different files. Spooling assumptions violated for " + dest;
                throw new IllegalStateException(message);
            }

            renamed = fileToRoll.renameTo(dest);
            if (!renamed) {
                message = "Unable to move " + fileToRoll + " to " + dest + ". This will likely cause duplicate events. Please verify that flume has sufficient permissions to perform these operations.";
                throw new FlumeException(message);
            }

            logger.debug("Successfully rolled file {} to {}", fileToRoll, dest);
            this.deleteMetaFile();
        }

    }

    private void deleteCurrentFile(File fileToDelete) throws IOException {
        logger.info("Preparing to delete file {}", fileToDelete);
        if (!fileToDelete.exists()) {
            logger.warn("Unable to delete nonexistent file: {}", fileToDelete);
        } else if (!fileToDelete.delete()) {
            throw new IOException("Unable to delete spool file: " + fileToDelete);
        } else {
            this.deleteMetaFile();
        }
    }

    private Optional<BriteCloudReliableSpoolingFileEventReader.FileInfo> getNextFile() {
        List<File> candidateFiles = Collections.emptyList();
        if (this.consumeOrder != ConsumeOrder.RANDOM || this.candidateFileIter == null || !this.candidateFileIter.hasNext()) {
            FileFilter filter = new FileFilter() {
                @Override
                public boolean accept(File candidate) {
                    String fileName = candidate.getName();
                    if (!candidate.isDirectory() && !fileName.endsWith(BriteCloudReliableSpoolingFileEventReader.this.completedSuffix) && !fileName.startsWith(".") && !BriteCloudReliableSpoolingFileEventReader.this.ignorePattern.matcher(fileName).matches()) {
                        return BriteCloudReliableSpoolingFileEventReader.this.undoKeepTime <= 0 || System.currentTimeMillis() - candidate.lastModified() >= (long) (BriteCloudReliableSpoolingFileEventReader.this.undoKeepTime * 1000);
                    } else {
                        return false;
                    }
                }
            };
            candidateFiles = Arrays.asList(this.spoolDirectory.listFiles(filter));
            ++this.listFilesCount;
            this.candidateFileIter = candidateFiles.iterator();
        }

        if (!this.candidateFileIter.hasNext()) {
            return Optional.absent();
        } else {
            File selectedFile = (File) this.candidateFileIter.next();
            if (this.consumeOrder == ConsumeOrder.RANDOM) {
                return this.openFile(selectedFile);
            } else {
                Iterator var3;
                File candidateFile;
                long compare;
                if (this.consumeOrder == ConsumeOrder.YOUNGEST) {
                    var3 = candidateFiles.iterator();

                    while (var3.hasNext()) {
                        candidateFile = (File) var3.next();
                        compare = selectedFile.lastModified() - candidateFile.lastModified();
                        if (compare == 0L) {
                            selectedFile = this.smallerLexicographical(selectedFile, candidateFile);
                        } else if (compare < 0L) {
                            selectedFile = candidateFile;
                        }
                    }
                } else {
                    var3 = candidateFiles.iterator();

                    while (var3.hasNext()) {
                        candidateFile = (File) var3.next();
                        compare = selectedFile.lastModified() - candidateFile.lastModified();
                        if (compare == 0L) {
                            selectedFile = this.smallerLexicographical(selectedFile, candidateFile);
                        } else if (compare > 0L) {
                            selectedFile = candidateFile;
                        }
                    }
                }

                return this.openFile(selectedFile);
            }
        }
    }

    private File smallerLexicographical(File f1, File f2) {
        return f1.getName().compareTo(f2.getName()) < 0 ? f1 : f2;
    }

    private Optional<BriteCloudReliableSpoolingFileEventReader.FileInfo> openFile(File file) {
        try {
            String nextPath = file.getPath();
            PositionTracker tracker = DurablePositionTracker.getInstance(this.metaFile, nextPath);
            if (!tracker.getTarget().equals(nextPath)) {
                tracker.close();
                this.deleteMetaFile();
                tracker = DurablePositionTracker.getInstance(this.metaFile, nextPath);
            }

            Preconditions.checkState(tracker.getTarget().equals(nextPath), "Tracker target %s does not equal expected filename %s", new Object[]{tracker.getTarget(), nextPath});
            ResettableInputStream in = new ResettableFileInputStream(file, tracker, 16384, this.inputCharset, this.decodeErrorPolicy);
            EventDeserializer deserializer = EventDeserializerFactory.getInstance(this.deserializerType, this.deserializerContext, in);
            return Optional.of(new BriteCloudReliableSpoolingFileEventReader.FileInfo(file, deserializer));
        } catch (FileNotFoundException var6) {
            logger.warn("Could not find file: " + file, var6);
            return Optional.absent();
        } catch (IOException var7) {
            logger.error("Exception opening file: " + file, var7);
            return Optional.absent();
        }
    }

    private void deleteMetaFile() throws IOException {
        if (this.metaFile.exists() && !this.metaFile.delete()) {
            throw new IOException("Unable to delete old meta file " + this.metaFile);
        }
    }

    public static class Builder {
        private File spoolDirectory;
        private String completedSuffix = "fileSuffix";
        private String ignorePattern = "^$";
        private String trackerDirPath = ".flumespool";
        private Boolean annotateFileName = false;
        private String fileNameHeader = "file";
        private Boolean annotateBaseName = false;
        private String baseNameHeader = "basename";
        private String deserializerType = "LINE";
        private Context deserializerContext = new Context();
        private String deletePolicy = "never";
        private String inputCharset = "UTF-8";
        private DecodeErrorPolicy decodeErrorPolicy;
        private ConsumeOrder consumeOrder;
        private int undoKeepTime;

        public Builder() {
            this.decodeErrorPolicy = DecodeErrorPolicy.valueOf(SpoolDirectorySourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY.toUpperCase(Locale.ENGLISH));
            this.consumeOrder = SpoolDirectorySourceConfigurationConstants.DEFAULT_CONSUME_ORDER;
            this.undoKeepTime = 0;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder spoolDirectory(File directory) {
            this.spoolDirectory = directory;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder completedSuffix(String completedSuffix) {
            this.completedSuffix = completedSuffix;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder ignorePattern(String ignorePattern) {
            this.ignorePattern = ignorePattern;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder trackerDirPath(String trackerDirPath) {
            this.trackerDirPath = trackerDirPath;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder annotateFileName(Boolean annotateFileName) {
            this.annotateFileName = annotateFileName;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder fileNameHeader(String fileNameHeader) {
            this.fileNameHeader = fileNameHeader;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder annotateBaseName(Boolean annotateBaseName) {
            this.annotateBaseName = annotateBaseName;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder baseNameHeader(String baseNameHeader) {
            this.baseNameHeader = baseNameHeader;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder deserializerType(String deserializerType) {
            this.deserializerType = deserializerType;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder deserializerContext(Context deserializerContext) {
            this.deserializerContext = deserializerContext;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder deletePolicy(String deletePolicy) {
            this.deletePolicy = deletePolicy;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder inputCharset(String inputCharset) {
            this.inputCharset = inputCharset;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder decodeErrorPolicy(DecodeErrorPolicy decodeErrorPolicy) {
            this.decodeErrorPolicy = decodeErrorPolicy;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder consumeOrder(ConsumeOrder consumeOrder) {
            this.consumeOrder = consumeOrder;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader.Builder undoKeepTime(int undoKeepTime) {
            this.undoKeepTime = undoKeepTime;
            return this;
        }

        public BriteCloudReliableSpoolingFileEventReader build() throws IOException {
            return new BriteCloudReliableSpoolingFileEventReader(this.spoolDirectory, this.completedSuffix, this.ignorePattern, this.trackerDirPath, this.annotateFileName.booleanValue(), this.fileNameHeader, this.annotateBaseName.booleanValue(), this.baseNameHeader, this.deserializerType, this.deserializerContext, this.deletePolicy, this.inputCharset, this.decodeErrorPolicy, this.consumeOrder, this.undoKeepTime, null);
        }
    }

    @Private
    @Unstable
    enum DeletePolicy {
        /**
         * never use
         */
        NEVER,
        /**
         * now use
         */
        IMMEDIATE,
        /**
         * day use
         */
        DELAY;

        DeletePolicy() {
        }
    }

    private static class FileInfo {
        private final File file;
        private final long length;
        private final long lastModified;
        private final EventDeserializer deserializer;

        public FileInfo(File file, EventDeserializer deserializer) {
            this.file = file;
            this.length = file.length();
            this.lastModified = file.lastModified();
            this.deserializer = deserializer;
        }

        public long getLength() {
            return this.length;
        }

        public long getLastModified() {
            return this.lastModified;
        }

        public EventDeserializer getDeserializer() {
            return this.deserializer;
        }

        public File getFile() {
            return this.file;
        }
    }
}

package org.hsqldb.lib;

import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

@ForSubject(ReaderEncodingInputStream.class)
@SuppressWarnings("ClassWithoutLogger")
public class ReaderEncodingInputStreamTest extends BaseTestCase {

    private static final int CHARS_LENGTH = 8 * 1024 * 1024;
    private static final char UNDEFINED_REPLACEMENT = '?';
    private static final char[] CHARS = computeChars(CHARS_LENGTH,
            UNDEFINED_REPLACEMENT);

    private static char[] computeChars(final int length,
            char undefinedReplacement) {
        final char[] chars = new char[length];
        final int start = 0;
        final int end = length - 1;
        for (int i = start; i <= end; i++) {
            chars[i] = Character.isDefined((char) i)
                    ? (char) i
                    : undefinedReplacement;
        }
        return chars;
    }

    public ReaderEncodingInputStreamTest(final String testName) {
        super(testName);
    }

    public Reader newReader() throws IOException {
        return new CharArrayReader(CHARS);
    }
    
    /**
     * Dummy test
     */
    public void testNothing() {
        
    }

    @OfMethod({"read()", "read(byte[],int,int)"})
    public void no_testAvailableCharsets() throws FileNotFoundException,
            IOException,
            Exception {
        final AtomicLong bytesReadCount = new AtomicLong();
        final AtomicInteger charsetsTestedCount = new AtomicInteger();
        final AtomicLong charsReadCount = new AtomicLong();
        final Collection<Charset> charsets = Charset.availableCharsets()
                .values();
        final Iterator<Charset> itr = charsets.stream().sorted().iterator();
        final StopWatch sw = new StopWatch(true);

        while (itr.hasNext()) {
            final Charset charset = itr.next();
            printProgress("Charset: " + charset.name()
                    + " " + charset.aliases());
            try {
                singleCharsetRun(charset, bytesReadCount);
                charsReadCount.addAndGet(CHARS_LENGTH);
                charsetsTestedCount.addAndGet(1);
            } catch (IOException | RuntimeException ex) {
                printWarning(ex);
            }
        }
        final long elapsedTime = sw.elapsedTime();
        printProgress(charsetsTestedCount.get() + " charsets tested");
        String msg = sw.currentElapsedTimeToMessage(
                bytesReadCount.get() + " bytes read");
        printProgress(msg);
        final double bytesPerMilli = bytesReadCount.get() / (double) elapsedTime;
        final double mbPerSecond = bytesPerMilli * 1000 / (1024 * 1024);
        msg = String.format(
                "Average byte read speed: %.2f MB / Second",
                mbPerSecond);
        printProgress(msg);
        long charsRead = charsReadCount.get();
        final double charsPerMilli = charsRead / (double) elapsedTime;
        final double mCharsPerSecond = charsPerMilli * 1000 / (1000 * 1000);
        msg = String.format(
                "average UFT16 char read speed: %.2f Million / Second",
                mCharsPerSecond);
        printProgress(msg);
    }

    private long singleCharsetRun(final Charset charset,
            final AtomicLong byteCount) throws IOException, Exception {
        final Reader reader = newReader();
        final InputStream actualIs = new ReaderEncodingInputStream(reader, charset);
        final ByteArrayOutputStream expectedBaos = new ByteArrayOutputStream();
        final OutputStreamWriter osw = new OutputStreamWriter(expectedBaos, charset);
        InOutUtil.copy(reader, osw);
        final byte[] expectedBytes = expectedBaos.toByteArray();
        final ByteArrayOutputStream actualBaos = new ByteArrayOutputStream();
        final long amount = Long.MAX_VALUE;
        final int bufferSize = 4096;
        final int reportEvery = CHARS.length;
        final long bytesCopied = copy(actualIs, actualBaos, amount, bufferSize, reportEvery);
        final byte[] actualBytes = actualBaos.toByteArray();
        assertJavaArrayEquals(expectedBytes, actualBytes);
        return byteCount.addAndGet(bytesCopied);
        
    }

    @SuppressWarnings("NestedAssignment")
    long copy(final InputStream inputStream,
            final OutputStream outputStream,
            final long amount,
            final int bufferSize,
            long reportEvery) throws IOException {
        int maxBytesToRead = (int) Math.min(bufferSize, amount);
        long nextReport = reportEvery;
        final byte[] buffer = new byte[maxBytesToRead];
        long bytesCopied = 0;
        int bytesRead;
        while ((bytesCopied < amount)
                && -1 != (bytesRead = inputStream.read(buffer, 0,
                        maxBytesToRead))) {
            outputStream.write(buffer, 0, bytesRead);
            if (bytesRead > Long.MAX_VALUE - bytesCopied) {
                bytesCopied = Long.MAX_VALUE;
            } else {
                bytesCopied += bytesRead;
            }
            if (bytesCopied >= amount) {
                break;
            }
            maxBytesToRead = (int) Math.min(bufferSize, amount - bytesCopied);
            if (reportEvery > 0 && bytesCopied >= nextReport) {
                nextReport += reportEvery;
                printProgress("bytes copied : "
                        + (bytesCopied / (float) (1024)) + " KB");
            }
        }
        if (reportEvery > 0) {
            printProgress("bytes copied : " + 
                    (bytesCopied / (float) (1024)) + " KB");
        }
        return bytesCopied;
    }
}

package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public final class LoggingHandlerBuilder {

    public static ILoggingHandler build(String identifier) {
        String fileName = identifier + "_" + System.currentTimeMillis() +".llog";
        Path path = getPath(fileName);
        FileChannel fileChannel;
        try {
            fileChannel = FileChannel.open(path,
                    StandardOpenOption.CREATE_NEW,
                    // StandardOpenOption.TRUNCATE_EXISTING,
                    // StandardOpenOption.SYNC,
                    StandardOpenOption.APPEND

            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String loggingType = System.getProperty("logging_type");
        ILoggingHandler handler;
        try {
            if(loggingType == null || loggingType.isEmpty() || loggingType.contentEquals("default")){
                handler = new DefaultLoggingHandler(fileChannel, fileName);
            } else {
                handler = new CompressedLoggingHandler(fileChannel, fileName);
            }
        } catch (NoClassDefFoundError | Exception e) {
            System.out.println("Failed to load compressed logging handler, setting the default. Error:\n"+e);
            handler = new DefaultLoggingHandler(fileChannel, fileName);
        }
        return handler;
    }

    public static Path getPath(String fileName) {
        String userHome = ConfigUtils.getUserHome();
        String basePath = userHome + "/vms";
        File file = new File(basePath);
        boolean fileExists = file.exists() || file.mkdirs();
        if(!fileExists){
            throw new RuntimeException("It was not possible to create the file "+file);
        }
        String filePath = basePath + "/" + fileName;
        return Paths.get(filePath);
    }

    public static RandomAccessFile setUpLoggingFile(String identifier, long threadId) {
        RandomAccessFile raf;
        String fileName = identifier + "_" + threadId + "_" + System.currentTimeMillis() +".llog";
        Path path = LoggingHandlerBuilder.getPath(fileName);
        try {
            raf = new RandomAccessFile(path.toFile(), "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return raf;
    }

    public static void writeToLog(RandomAccessFile raf, TransactionEvent.PayloadRaw payload) {
        try {
            writeLong(raf, payload.tid());
            writeLong(raf, payload.batch());
            raf.write(payload.event().length);
            raf.write(payload.event());
            raf.write(payload.payload().length);
            raf.write(payload.payload());
            raf.write(payload.precedenceMap().length);
            raf.write(payload.precedenceMap());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeLong(RandomAccessFile raf, long lng) throws IOException {
        raf.write(new byte[] {
                (byte) lng,
                (byte) (lng >> 8),
                (byte) (lng >> 16),
                (byte) (lng >> 24),
                (byte) (lng >> 32),
                (byte) (lng >> 40),
                (byte) (lng >> 48),
                (byte) (lng >> 56)});
    }

}

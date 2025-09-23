package bdpuh.hw03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.*;
import java.util.zip.GZIPOutputStream;
import java.util.stream.Collectors;

public class ParallelLocalToHdfsCopy {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ParallelLocalToHdfsCopy <localSrcDir> <hdfsDestDir> <threads>");
            System.exit(1);
        }

        final java.nio.file.Path localSrc = Paths.get(args[0]).toAbsolutePath();
        final String hdfsDestStr = args[1];
        final int threads = Integer.parseInt(args[2]);

        if (!Files.exists(localSrc) || !Files.isDirectory(localSrc)) {
            System.err.println("Source directory does not exist");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(hdfsDestStr), conf);
        Path hdfsDest = new Path(hdfsDestStr);

        if (fs.exists(hdfsDest)) {
            System.err.println("Destination directory already exists. Please delete before running the program");
            System.exit(3);
        }
        if (!fs.mkdirs(hdfsDest)) {
            System.err.println("Failed to create destination directory on HDFS: " + hdfsDest);
            System.exit(4);
        }

        List<java.nio.file.Path> files;
        try (java.util.stream.Stream<java.nio.file.Path> s = Files.list(localSrc)) {
            files = s.filter(Files::isRegularFile).collect(Collectors.toList());
        }

        if (files.isEmpty()) {
            System.out.println("No regular files found to copy.");
            System.exit(0);
        }

        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, threads));
        CompletionService<String> ecs = new ExecutorCompletionService<>(pool);

        for (java.nio.file.Path f : files) {
            ecs.submit(() -> {
                String fname = f.getFileName().toString();
                Path destFile = new Path(hdfsDest, fname + ".gz"); 
                try (
                    InputStream in = Files.newInputStream(f);
                    FSDataOutputStream hdfsOut = fs.create(destFile, false);
                    BufferedOutputStream bos = new BufferedOutputStream(hdfsOut, 128 * 1024);
                    GZIPOutputStream gz = new GZIPOutputStream(bos)
                ) {
                    byte[] buf = new byte[128 * 1024];
                    int n;
                    while ((n = in.read(buf)) >= 0) {
                        gz.write(buf, 0, n);
                    }
                    gz.finish();
                    System.out.println("Copied & compressed: " + f + " -> " + destFile);
                    return destFile.toString();
                } catch (Exception e) {
                    System.err.println("Failed copying " + f + ": " + e.getMessage());
                    throw e;
                }
            });
        }

        pool.shutdown();
        for (int i = 0; i < files.size(); i++) {
            try { ecs.take().get(); } catch (ExecutionException ee) { /* already logged */ }
        }
        System.out.println("All done.");
    }
}

package data_algorithms_book.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class HadoopUtil {
	public static void addJarsToDistributedCache(Job job, String hdfsJarDirectory) throws IOException {
		if (job == null) {
			return;
		}
		addJarsToDistributedCache(job.getConfiguration(), hdfsJarDirectory);
	}
	
	public static void addJarsToDistributedCache(Configuration conf, String hdfsJarDirectory) throws IOException {
		if (conf == null){
			return;
		}
		FileSystem fs = FileSystem.get(conf);
		List<FileStatus> jars = getDirectoryListing(hdfsJarDirectory, fs);
		for (FileStatus jar : jars) {
			Path jarPath = jar.getPath();
			DistributedCache.addFileToClassPath(jarPath, conf, fs);
		}
	}
	
	public static List<FileStatus> getDirectoryListing(String directory, FileSystem fs) throws FileNotFoundException, IOException {
		Path dir = new Path(directory);
		FileStatus[] fstatus = fs.listStatus(dir);
		return Arrays.asList(fstatus);
	}
	
	public static List<String> listDirectoryAsListOfString(String directory, FileSystem fs) throws FileNotFoundException, IOException{
		Path path = new Path(directory);
		FileStatus[] fstatus = fs.listStatus(path);
		List<String> listing = new ArrayList<String>();
		for (FileStatus f : fstatus) {
			listing.add(f.getPath().toUri().getPath());
		}
		return listing;
	}
	
	public static boolean pathExists(Path path, FileSystem fs) {
		if (path == null) {
			return false;
		}
		
		try {
			return fs.exists(path);
		} catch (Exception e) {
			return false;
		}
	}
}

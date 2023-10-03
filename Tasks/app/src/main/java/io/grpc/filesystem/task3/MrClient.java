/* 
* Client program to request for map and reduce functions from the Server
*/

package io.grpc.filesystem.task3;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.task3.proto.AssignJobGrpc;
import com.task3.proto.MapInput;
import com.task3.proto.ReduceInput;
import com.task3.proto.MapOutput;
import com.task3.proto.ReduceOutput;
import io.grpc.filesystem.task2.*;

import java.io.*;
import java.nio.charset.Charset;

public class MrClient {

   Map<String, Integer> jobStatus = new HashMap<String, Integer>();

   public  void requestMap(String ip, Integer portnumber, String inputfilepath, String outputfilepath) throws InterruptedException {

      ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, portnumber).usePlaintext().build();
      AssignJobGrpc.AssignJobStub newStub = AssignJobGrpc.newStub(channel);

      CountDownLatch latch = new CountDownLatch(1);

      StreamObserver<MapInput> serverResponse = newStub.map(new StreamObserver<MapOutput>() {
         @Override
         public void onNext(MapOutput value) {
            jobStatus.put(inputfilepath, value.getJobstatus());
            System.out.println("Received response: Job status: " + value.getJobstatus());
         }

         @Override
         public void onError(Throwable t) {
            System.err.println("Error occurred: " + t.getMessage());
            latch.countDown();
         }

         @Override
         public void onCompleted() {
            System.out.println("Element successfully mapped");
            latch.countDown();
         }
      });
      serverResponse.onNext(MapInput.newBuilder().setInputfilepath(inputfilepath).build());
      serverResponse.onCompleted();

      try {
         latch.await();
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
      channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
   }

   public int requestReduce(String ip, Integer portnumber, String inputfilepath, String outputfilepath) throws InterruptedException{
      ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, portnumber).usePlaintext().build();
      AssignJobGrpc.AssignJobBlockingStub blockingStub = AssignJobGrpc.newBlockingStub(channel);

      ReduceInput newInput = ReduceInput.newBuilder()
              .setInputfilepath(inputfilepath)
              .setOutputfilepath(outputfilepath)
              .build();

      return blockingStub.reduce(newInput).getJobstatus();
   }

   public static void main(String[] args) throws Exception {// update main function if required

      String ip = args[0];
      Integer mapport = Integer.parseInt(args[1]);
      Integer reduceport = Integer.parseInt(args[2]);
      String inputfilepath = args[3];
      String outputfilepath = args[4];
      String jobtype = null;
      MrClient client = new MrClient();
      int response = 0;

      MapReduce mr = new MapReduce();
      String chunkpath = mr.makeChunks(inputfilepath);
      Integer noofjobs = 0;
      File dir = new File(chunkpath);
      File[] directoyListing = dir.listFiles();
      if (directoyListing != null) {
         for (File f : directoyListing) {
            if (f.isFile()) {
               noofjobs += 1;
               client.jobStatus.put(f.getPath(), 1);
               client.requestMap(ip, mapport, f.getPath(), outputfilepath);
               //moved requestMap call up to avoid concurrency problems and to not have to loop a second time in the function
               //inputfilepath is now path to single chunk file and not path to all chunk files
            }
         }
      }

      Set<Integer> values = new HashSet<Integer>(client.jobStatus.values());
      if (values.size() == 1 && client.jobStatus.containsValue(2)) {

         response = client.requestReduce(ip, reduceport, chunkpath, outputfilepath);
         if (response == 2) {

            System.out.println("Reduce task completed!");

         } else {
            System.out.println("Try again! " + response);
         }

      }

   }

}

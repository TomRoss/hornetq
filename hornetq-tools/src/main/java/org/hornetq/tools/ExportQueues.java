/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tools;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.persistence.impl.journal.JournalRecordIds;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;

/**
 * Writes a human-readable interpretation of the contents of a HornetQ {@link org.hornetq.core.journal.Journal}.
 * <p>
 * To run this class with Maven, use:
 * <p>
 * <pre>
 * cd hornetq-server
 * mvn -q exec:java -Dexec.args="/foo/hornetq/bindings /foo/hornetq/journal" -Dexec.mainClass="org.hornetq.tools.PrintData"
 * </pre>
 *
 * @author clebertsuconic
 */
public class ExportQueues // NO_UCD (unused code)
{

   protected static void exportQueues(String bindingsDirectory)
   {
      try
      {
         printQueues(bindingsDirectory);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }


   public static void printQueues(final String bindingsDir) throws Exception
   {

      SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(bindingsDir, null);

      JournalImpl journal = new JournalImpl(1024 * 1024, 2, -1, 0, bindingsFF, "hornetq-bindings", "bindings", 1);

      String legacyPostfix = System.getProperty("legacy.postfix","");
      String eap7Postfix = System.getProperty("eap7.postfix","");


      List<RecordInfo> records = new LinkedList<RecordInfo>();
      List<PreparedTransactionInfo> preparedTransactions = new LinkedList<PreparedTransactionInfo>();

      journal.start();

      journal.load(records, preparedTransactions, new TransactionFailureCallback()
      {

         @Override
         public void failedTransaction(long transactionID, List<RecordInfo> records1, List<RecordInfo> recordsToDelete)
         {
         }
      }, false);

      //String queueConfig =  "<jms-queue name=%0 legacy-entries=java:/jms/queue/inQueue java:jboss/exported/jms/queue/inQueue entries=java:/jms/queue/inQueue-new java:jboss/exported/jms/queue/inQueue-new/>";

      //StringBuilder jndiEntry = null;
      List<String> jndiEntries = new ArrayList<String>();
      Map<String,String> durableSubscribers = new HashMap<String,String>();

      for (RecordInfo info : records)
      {
         if (info.getUserRecordType() == JournalRecordIds.QUEUE_BINDING_RECORD)
         {
            HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(info.data);
            JournalStorageManager.PersistentQueueBindingEncoding bindingEncoding = new JournalStorageManager.PersistentQueueBindingEncoding();

            bindingEncoding.decode(buffer);

            bindingEncoding.setId(info.id);

            // TODO make the formatting here
            //System.out.println("##########################");
            //System.out.println("The address Name is:" + bindingEncoding.address);
            //System.out.println("The queue Name is:" + bindingEncoding.getQueueName());

            if (isQueue(bindingEncoding))
            {
               //System.out.println("QUEUE");

               //String str = String.format("<jms-queue name=\"%s1\" legacy-entries=\"java:/jms/queue/%s2 java:jboss/exported/jms/queue/%s%s1\" entries=\"java:/jms/queue/%s-%s2 java:jboss/exported/jms/queue/%s-%s2\">", bindingEncoding.getQueueName(), bindingEncoding.getQueueName(), bindingEncoding.getQueueName(), bindingEncoding.getQueueName(), bindingEncoding.getQueueName());
               //System.out.println(createdJndiEntry(stripBning(bindingEncoding.getQueueName().toString()),"QUEUE",legacyPostfix,eap7Postfix));
               //System.out.println(str);
               jndiEntries.add(createdJndiEntry(stripBinding(bindingEncoding.getQueueName().toString()),"QUEUE",legacyPostfix,eap7Postfix));
            }

            if (isTopic(bindingEncoding))
            {
               //System.out.println("TOPIC");

               //stripBning(bindingEncoding.getQueueName().toString());
               //String str = String.format("<jms-topic name=\"%1s\" legacy-entries=\"java:/jms/topic/%2s-%3s java:jboss/exported/jms/topic/%4s-%5s\" entries=\"java:/jms/topic/%6s-%7s java:jboss/exported/jms/topic/%8s-%9s\"/>", bindingEncoding.getQueueName().toString(), bindingEncoding.getQueueName().toString(),legacyPostfix, bindingEncoding.getQueueName().toString(), legacyPostfix, bindingEncoding.getQueueName().toString(),eap7Postfix,bindingEncoding.getQueueName().toString(),eap7Postfix);

               //System.out.println(str);

               //System.out.println(createdJndiEntry(stripBning(bindingEncoding.getQueueName().toString()),"TOPIC",legacyPostfix,eap7Postfix));

               if (isDurable(bindingEncoding))
               {
                  createSubscriber(durableSubscribers,bindingEncoding);
               }
               else
               {

                  jndiEntries.add(createdJndiEntry(stripBinding(bindingEncoding.getQueueName().toString()),"TOPIC",legacyPostfix,eap7Postfix));
               }


            }
         }


      }

      journal.stop();

      for (int i = 0; i < jndiEntries.size(); i++)
      {

         System.out.println(jndiEntries.get(i));
      }

      Iterator it = durableSubscribers.entrySet().iterator();

      while (it.hasNext())
      {
         Map.Entry pair = (Map.Entry)it.next();

         System.out.println("ClientID = " + pair.getKey() + ": Durable Subscriber = " + pair.getValue());
         it.remove(); // avoids a ConcurrentModificationException
      }

   }

   private static boolean isQueue(JournalStorageManager.PersistentQueueBindingEncoding destinationBidning )
   {

      SimpleString address = destinationBidning.getAddress();

      if (address.startsWith(SimpleString.toSimpleString("jms.queue")))
      {
         return true;
      }

      return false;
   }

   private static boolean isTopic(JournalStorageManager.PersistentQueueBindingEncoding destinationBinding )
   {

      SimpleString address = destinationBinding.getAddress();

      if (address.startsWith(SimpleString.toSimpleString("jms.topic")))
      {
         return true;
      }

      return false;
   }

   private static boolean isDurable(JournalStorageManager.PersistentQueueBindingEncoding destinationBinding )
   {
      System.out.println(destinationBinding.toString());

      String queueName = destinationBinding.getQueueName().toString();

      if (queueName.equals(destinationBinding.getAddress().toString()))
      {
         return false;
      }


      return true;
   }


   private static void createSubscriber(Map<String,String> subscribers, JournalStorageManager.PersistentQueueBindingEncoding destinationBinding)
   {
      String queueName = destinationBinding.getQueueName().toString();

      String[] s = queueName.split("\\.");

      subscribers.put(s[0],s[1]);

   }
   private static String stripBinding(String binding)
   {

      StringBuilder bindingName = new StringBuilder();

      String[] s = binding.split("\\.");

      int i = s.length;

      bindingName.append(s[i - 1]);

      return bindingName.toString();
   }

   private static String createdJndiEntry(String binding, String bindingType,String oldPost, String newPost)
   {

      StringBuilder jndiEntry = new StringBuilder();

      if (bindingType.equals("TOPIC"))
      {

         jndiEntry.append("<jms-topic name=");
         jndiEntry.append("\"");
         jndiEntry.append(binding);
         jndiEntry.append("\"");
         //jndiEntry.append(" legacy-entries=\"java:/jms/topic/");
         jndiEntry.append(" legacy-entries=");
         jndiEntry.append("\"");
         //jndiEntry.append(binding);
         //jndiEntry.append("-");
         //jndiEntry.append(oldPost);

         jndiEntry.append("java:jboss/exported/jms/topic/");
         jndiEntry.append(binding);
         jndiEntry.append("-");
         jndiEntry.append(oldPost);
         jndiEntry.append("\"");

         //jndiEntry.append(" entries=\"java:/jms/topic/");
         jndiEntry.append(" entries=");
         jndiEntry.append("\"");
         //jndiEntry.append(binding);
         //jndiEntry.append("-");
         //jndiEntry.append(newPost);
         jndiEntry.append("java:jboss/exported/jms/topic/");
         jndiEntry.append(binding);
         jndiEntry.append("-");
         jndiEntry.append(newPost);
         jndiEntry.append("\" />");

      }
      else if (bindingType.equals("QUEUE"))
      {

         jndiEntry.append("<jms-queue name=");
         jndiEntry.append("\"");
         jndiEntry.append(binding);
         jndiEntry.append("\"");
         jndiEntry.append(" legacy-entries=");
         jndiEntry.append("\"");
         //jndiEntry.append(" legacy-entries=\"java:/jms/queue/");
         //jndiEntry.append(binding);
         //jndiEntry.append("-");
         //jndiEntry.append(oldPost);

         jndiEntry.append("java:jboss/exported/jms/queue/");
         jndiEntry.append(binding);
         jndiEntry.append("-");
         jndiEntry.append(oldPost);
         jndiEntry.append("\"");

         //jndiEntry.append(" entries=\"java:/jms/queue/");
         jndiEntry.append(" entries=");
         jndiEntry.append("\"");
         //jndiEntry.append(binding);
         //jndiEntry.append("-");
         //jndiEntry.append(newPost);
         jndiEntry.append("java:jboss/exported/jms/queue/");
         jndiEntry.append(binding);
         jndiEntry.append("-");
         jndiEntry.append(newPost);
         jndiEntry.append("\" />");
      }

      return jndiEntry.toString();
   }
}

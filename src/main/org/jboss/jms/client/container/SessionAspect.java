/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.jms.client.container;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.jms.IllegalStateException;
import javax.jms.Session;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.server.endpoint.DefaultCancel;
import org.jboss.jms.server.endpoint.DefaultAck;
import org.jboss.jms.server.endpoint.DeliveryInfo;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.Util;

/**
 * This aspect handles JMS session related logic
 * 
 * This aspect is PER_VM
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.com>Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class SessionAspect
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(SessionAspect.class);
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   private void ackDelivery(SessionDelegate sess, DeliveryInfo delivery) throws Exception
   {
      SessionDelegate connectionConsumerSession = delivery.getConnectionConsumerSession();
      
      //If the delivery was obtained via a connection consumer we need to ack via that
      //otherwise we just use this session
      
      SessionDelegate sessionToUse = connectionConsumerSession != null ? connectionConsumerSession : sess;
      
      sessionToUse.acknowledgeDelivery(delivery);      
   }
   
   private void cancelDelivery(SessionDelegate sess, DeliveryInfo delivery) throws Exception
   {
      SessionDelegate connectionConsumerSession = delivery.getConnectionConsumerSession();
      
      //If the delivery was obtained via a connection consumer we need to cancel via that
      //otherwise we just use this session
      
      SessionDelegate sessionToUse = connectionConsumerSession != null ? connectionConsumerSession : sess;
      
      sessionToUse.cancelDelivery(new DefaultCancel(delivery.getDeliveryId(), delivery.getMessageProxy().getDeliveryCount()));      
   }
   
   public Object handleClosing(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
      
      int ackMode = state.getAcknowledgeMode();
  
      //We need to either ack (for auto_ack) or cancel (for client_ack)
      //any deliveries - this is because the message listener might have closed
      //before on message had finished executing
      
      if (ackMode == Session.AUTO_ACKNOWLEDGE ||
          ackMode == Session.DUPS_OK_ACKNOWLEDGE ||
          (state.isXA() && state.getCurrentTxId() == null))
      {
         //Acknowledge any outstanding auto ack
         
         DeliveryInfo remainingAutoAck = state.getAutoAckInfo();
         
         if (remainingAutoAck != null)
         {
            if (trace) { log.trace(this + " handleClosing(). Found remaining auto ack. Will ack it " + remainingAutoAck.getDeliveryId()); }
            
            ackDelivery(del, remainingAutoAck);
            
            if (trace) { log.trace(this + " acked it"); }
            
            state.setAutoAckInfo(null);
         }
      }
      else if (ackMode == Session.CLIENT_ACKNOWLEDGE)
      {
         // Cancel any oustanding deliveries
         // We cancel any client ack or transactional, we do this explicitly so we can pass the updated
         // delivery count information from client to server. We could just do this on the server but
         // we would lose delivery count info.
         
         //CLIENT_ACKNOWLEDGE cannot be used with MDBs so is always safe to cancel on this session
         
         List cancels = new ArrayList();
         
         for(Iterator i = state.getClientAckList().iterator(); i.hasNext(); )
         {
            DeliveryInfo ack = (DeliveryInfo)i.next();            
            DefaultCancel cancel = new DefaultCancel(ack.getMessageProxy().getDeliveryId(), ack.getMessageProxy().getDeliveryCount());
            cancels.add(cancel);
         }
         
         state.getClientAckList().clear();
      }
      
      return invocation.invokeNext();
   }


   public Object handleClose(Invocation invocation) throws Throwable
   {      
      Object res = invocation.invokeNext();

      // We must explicitly shutdown the executor

      SessionState state = getState(invocation);
      state.getExecutor().shutdownNow();

      return res;
   }
   
   public Object handlePreDeliver(Invocation invocation) throws Throwable
   { 
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);
      
      int ackMode = state.getAcknowledgeMode();
      
      Object[] args = mi.getArguments();
      DeliveryInfo info = (DeliveryInfo)args[0];
      
      if (ackMode == Session.CLIENT_ACKNOWLEDGE)
      {
         // We collect acknowledgments in the list
         
         if (trace) { log.trace(this + " delivery id: " + info.getDeliveryId() + " added to client ack list"); }
         
         state.getClientAckList().add(info);
         
         //We can return immediately
         return null;
      }
      else if (ackMode == Session.AUTO_ACKNOWLEDGE ||
               ackMode == Session.DUPS_OK_ACKNOWLEDGE ||
               (state.isXA() && state.getCurrentTxId() == null))
      {
         //We collect the single acknowledgement in the state.
         //Currently DUPS_OK is treated the same as AUTO_ACKNOWLDGE
         //Also XA sessions not enlisted in a global tx are treated as AUTO_ACKNOWLEDGE
         
         if (trace) { log.trace(this + " delivery id: " + info.getDeliveryId() + " added to client ack member"); }
         
         state.setAutoAckInfo(info);         
         
         //We can return immediately         
         return null;
      }
              
      //Transactional - need to carry on down the stack
      return invocation.invokeNext();
   }
   
   /* Used for client acknowledge */
   public Object handleAcknowledgeAll(Invocation invocation) throws Throwable
   {    
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
    
      if (!state.getClientAckList().isEmpty())
      {                 
         //CLIENT_ACKNOWLEDGE can't be used with a MDB so it is safe to always acknowledge all
         //on this session (rather than the connection consumer session)
         del.acknowledgeDeliveries(state.getClientAckList());
      
         state.getClientAckList().clear();
      }
        
      return null;
   }
   
   public Object handlePostDeliver(Invocation invocation) throws Throwable
   { 
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);
      
      int ackMode = state.getAcknowledgeMode();
      
      boolean cancel = ((Boolean)mi.getArguments()[0]).booleanValue();
      
      if (cancel && ackMode != Session.AUTO_ACKNOWLEDGE && ackMode != Session.DUPS_OK_ACKNOWLEDGE)
      {
         throw new IllegalStateException("Ack mode must be AUTO_ACKNOWLEDGE or DUPS_OK_ACKNOWLEDGE");
      }
      
      if (ackMode == Session.AUTO_ACKNOWLEDGE ||
          ackMode == Session.DUPS_OK_ACKNOWLEDGE ||
          (state.isXA() && state.getCurrentTxId() == null))
      {
         //We auto acknowledge
         //Currently DUPS_OK is treated the same as AUTO_ACKNOWLDGE
         //Also XA sessions not enlisted in a global tx are treated as AUTO_ACKNOWLEDGE
         
         SessionDelegate sd = (SessionDelegate)mi.getTargetObject();

         if (!state.isRecoverCalled())
         {
            DeliveryInfo deliveryInfo = state.getAutoAckInfo();
            
            if (deliveryInfo == null)
            {
               throw new IllegalStateException("Cannot find delivery to auto ack");
            }
                                 
            if (trace) { log.trace(this + " auto acking delivery " + deliveryInfo.getDeliveryId()); }
                        
            if (cancel)
            {
               cancelDelivery(sd, deliveryInfo);
            }
            else
            {
               ackDelivery(sd, deliveryInfo);
            }
            
            state.setAutoAckInfo(null);
         }
         else
         {
            if (trace) { log.trace("recover called, so NOT acknowledging"); }

            state.setRecoverCalled(false);
         }
      }

      return null;
   }
                  
   /*
    * Called when session.recover is called
    */
   public Object handleRecover(Invocation invocation) throws Throwable
   {
      if (trace) { log.trace("recover called"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
            
      SessionState state = getState(invocation);
      
      if (state.isTransacted())
      {
         throw new IllegalStateException("Cannot recover a transacted session");
      }
      
      if (trace) { log.trace("recovering the session"); }
       
      //Call redeliver
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
      
      if (state.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE)
      {
         del.redeliver(state.getClientAckList());
         
         state.getClientAckList().clear();
      }
      else
      {
         DeliveryInfo info = state.getAutoAckInfo();
         
         if (info != null)
         {
            List redels = new ArrayList();
            
            redels.add(info);
            
            del.redeliver(redels);
            
            state.setAutoAckInfo(null);            
         }
      }            

      state.setRecoverCalled(true);
      
      return null;  
   }
   
   /**
    * Redelivery occurs in two situations:
    *
    * 1) When session.recover() is called (JMS1.1 4.4.11)
    *
    * "A session's recover method is used to stop a session and restart it with its first
    * unacknowledged message. In effect, the session's series of delivered messages is reset to the
    * point after its last acknowledged message."
    *
    * An important note here is that session recovery is LOCAL to the session. Session recovery DOES
    * NOT result in delivered messages being cancelled back to the channel where they can be
    * redelivered - since that may result in them being picked up by another session, which would
    * break the semantics of recovery as described in the spec.
    *
    * 2) When session rollback occurs (JMS1.1 4.4.7). On rollback of a session the spec is clear
    * that session recovery occurs:
    *
    * "If a transaction rollback is done, its produced messages are destroyed and its consumed
    * messages are automatically recovered. For more information on session recovery, see Section
    * 4.4.11 'Message Acknowledgment.'"
    *
    * So on rollback we do session recovery (local redelivery) in the same as if session.recover()
    * was called.
    */
   public Object handleRedeliver(Invocation invocation) throws Throwable
   {            
      MethodInvocation mi = (MethodInvocation)invocation;
      SessionState state = getState(invocation);
            
      // We put the messages back in the front of their appropriate consumer buffers and set
      // JMSRedelivered to true.
      
      List toRedeliver = (List)mi.getArguments()[0];
      
      if (trace) { log.trace(this + " handleRedeliver() called: " + toRedeliver); }
      
      SessionDelegate del = (SessionDelegate)mi.getTargetObject();
      
      // Need to be recovered in reverse order.
      for (int i = toRedeliver.size() - 1; i >= 0; i--)
      {
         DeliveryInfo info = (DeliveryInfo)toRedeliver.get(i);
         MessageProxy proxy = info.getMessageProxy();        
         
         MessageCallbackHandler handler = state.getCallbackHandler(info.getConsumerId());
              
         if (handler == null)
         {
            // This is ok. The original consumer has closed, so we cancel the message
            
            cancelDelivery(del, info);
         }
         else
         {
            if (trace) { log.trace("Adding proxy back to front of buffer"); }
            handler.addToFrontOfBuffer(proxy);
         }                                    
      }
              
      return null;  
   }
   
   public Object handleGetXAResource(Invocation invocation) throws Throwable
   {
      return getState(invocation).getXAResource();
   }
   
   public Object handleGetTransacted(Invocation invocation) throws Throwable
   {
      return getState(invocation).isTransacted() ? Boolean.TRUE : Boolean.FALSE;
   }
   
   public Object handleGetAcknowledgeMode(Invocation invocation) throws Throwable
   {
      return new Integer(getState(invocation).getAcknowledgeMode());
   }
   

   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   private SessionState getState(Invocation inv)
   {
      return (SessionState)((DelegateSupport)inv.getTargetObject()).getState();
   }

   // Inner Classes -------------------------------------------------
   
}


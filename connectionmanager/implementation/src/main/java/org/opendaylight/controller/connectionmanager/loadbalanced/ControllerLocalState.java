package org.opendaylight.controller.connectionmanager.loadbalanced;

public enum ControllerLocalState {
	 BUSY("BUSY"), NORMAL("NORMAL"), IDLE("IDLE"), HIBERNATE("HIBERNATE");
	 private String name; 
	 private ControllerLocalState(String name) {  
         this.name = name;  
     }  
   
     @Override  
     public String toString() {  
         return this.name;  
     }  
}
package org.opendaylight.controller.connectionmanager.loadbalanced;

public enum DERSSetting {
	 LW("LW"), PI("PI");
	 private String name; 
	 private DERSSetting(String name) {  
        this.name = name;  
    }  
  
    @Override  
    public String toString() {  
        return this.name;  
    }  
    
    public static DERSSetting fromString(String name) {
    	for( DERSSetting s: DERSSetting.values()) {
            if (s.toString().equals(name)) {
                return s;
            }
    	}
    	return null;
    }

}

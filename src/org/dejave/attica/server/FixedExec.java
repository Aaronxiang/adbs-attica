package org.dejave.attica.server;

import org.dejave.attica.engine.operators.Sink;
import org.dejave.attica.storage.Tuple;



public class FixedExec {
	/**
	 * @param args
	 */
	public static void main(String[] args) {		
        // construct the db instance
        try {
			Database db = new Database("/home/krzys/Documents/University/adbs/attica-src/attica.properties", false);
            Sink sink = db.runStatement("select a.s, b.s from a, b where a.f=b.f and a.f=b.f");

            for (Tuple tuple : sink.tuples())
                System.out.println(tuple.toStringFormatted());
            

            db.shutdown();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
	}
}

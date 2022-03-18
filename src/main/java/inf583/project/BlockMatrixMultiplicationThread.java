package inf583.project;

import java.util.ArrayList;
import java.util.HashSet;

public class BlockMatrixMultiplicationThread implements Runnable {
	
	private ArrayList<HashSet<Integer>> edgeList;
	private int startI, endI, startJ, endJ;
	private double[] blockB;
	private double[] result;
	
	public BlockMatrixMultiplicationThread(ArrayList<HashSet<Integer>> edgeList, double[] r, int blockA, double[] result) {
		// blockA = 0 -> upper left block | blockA = 1 -> upper right block
		// blockA = 2 -> lower left block | blockA = 3 -> lower right block
		
		this.result = result;
		this.edgeList = edgeList;
		
		// We suppose that A is a square matrix
		int split = edgeList.size() / 2;
		
		// Now we construct blockA from edgeList
		if(blockA == 0) {
			startI = 0;
			endI = split;
			startJ = 0;
			endJ = split;
		} else if(blockA == 1) {
			startI = 0;
			endI = split;
			startJ = split;
			endJ = edgeList.size();
		} else if(blockA == 2) {
			startI = split;
			endI = edgeList.size();
			startJ = 0;
			endJ = split;
		} else { // blockA == 3
			startI = split;
			endI = edgeList.size();
			startJ = split;
			endJ = edgeList.size();
		}
		// Now we construct blockB from r
		
		this.blockB = new double[endJ - startJ];	
		for (int j = startJ; j < endJ; j++) {
			blockB[j - startJ] = r[j];
		}
	}
	
    @Override
    public void run() {
        for (int i = startI; i < endI; i++) {
			result[i - startI] = 0;
			for (int j = startJ; j < endJ; j++) {
				if (edgeList.get(i).contains(j)) {
					result[i - startI] +=  blockB[j - startJ];
				}
			}
		}
    }
}

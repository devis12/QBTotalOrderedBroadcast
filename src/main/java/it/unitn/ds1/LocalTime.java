package it.unitn.ds1;

public class LocalTime{
    //current epoch
    public int epoch;
    //current sn
    public int sn;

    public LocalTime(int epoch, int sn){
        this.epoch = epoch;
        this.sn = sn;
    }

    //new sequence within the current epoch
    public void newSequence(){ sn++; }

    //starts a new epoch
    public void newEpoch(){ epoch++; sn=0; }
}
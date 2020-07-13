package it.unitn.ds1;

import java.io.Serializable;

public final class LocalTime implements Serializable{
    //current epoch
    public final int epoch;
    //current sn
    public final int sn;

    //new sequence within the current epoch
    public static LocalTime newSequence(LocalTime clock){
        return new LocalTime(clock.epoch, clock.sn+1);
    }

    //starts a new epoch
    public static LocalTime newEpoch(LocalTime clock){
        return new LocalTime(clock.epoch+1, 0);
    }

    public LocalTime(int epoch, int sn){
        this.epoch = epoch;
        this.sn = sn;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LocalTime))
            return false;

        LocalTime clock2 = (LocalTime) obj;
        return this.epoch == clock2.epoch && this.sn == clock2.sn;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 53 * hash + this.epoch;
        hash = 53 * hash + this.sn;
        return hash;
    }

    @Override
    public String toString(){
        return  this.epoch + ":" + this.sn ;
    }
}
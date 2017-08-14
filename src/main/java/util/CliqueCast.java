package util;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Thomas on 16/6/2017.
 */
public class CliqueCast {
    private int actor;
    private List<Integer> casts;

    public CliqueCast(String actor, String casts) {
        this.actor = Integer.parseInt(actor);
        this.casts = new ArrayList<Integer>();

        for (String cast : casts.split(",")) {
            this.casts.add(Integer.parseInt(cast));
        }
    }

    public boolean isIncluded(String actors) {
        for (String actor : actors.split(",")) {
            if ( Integer.parseInt(actor) == this.actor) {
                return true;
            }
        }

        return false;
    }

    public boolean isCoActedGreaterThanOrEqualToN(String casts, int n) {
        String[] casts_str_arr = casts.split(",");
        int[] casts_arr = new int[casts_str_arr.length];

        for (int i = 0; i < casts_str_arr.length; i++) {
            casts_arr[i] = Integer.parseInt(casts_str_arr[i]);
        }

        int count = 0;

        int idx_a = 0;
        int idx_b = 0;

        int length_a = this.casts.size();
        int length_b = casts_arr.length;


        // assume both array are sorted.
        while (idx_a < length_a && idx_b < length_b) {
            if (this.casts.get(idx_a) == casts_arr[idx_b]) {
                count++;
                idx_a++;
                idx_b++;
            } else if (this.casts.get(idx_a) < casts_arr[idx_b]) {
                idx_a++;
            } else {
                idx_b++;
            }

            if (count >= n) {
                return true;
            }
        }

        return false;
    }

    public String[] combineActorIntoClique(String actors, String casts){
        String[] casting = new String[2];

        List<Integer> new_actors = new ArrayList<Integer>();
        for (String actor : actors.split(",")) {
            new_actors.add(Integer.parseInt(actor));
        }
        new_actors.add(this.actor);
        Collections.sort(new_actors);
        casting[0] = Joiner.on(",").join(new_actors);

        int idx_a = 0;
        int idx_b = 0;

        List<Integer> new_casts = new ArrayList<Integer>();
        String[] casts_arr = casts.split(",");

        while (idx_a < this.casts.size() && idx_b < casts_arr.length) {
            int a = this.casts.get(idx_a);
            int b = Integer.parseInt(casts_arr[idx_b]);
            if (a == b) {
                new_casts.add(a);
                idx_a++;
                idx_b++;
            } else if (a < b) {
                idx_a++;
            } else {
                idx_b++;
            }
        }

        Collections.sort(new_casts);
        casting[1] = Joiner.on(",").join(new_casts);

        return casting;
    }
}

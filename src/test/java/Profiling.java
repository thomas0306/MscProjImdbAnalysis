import chaining.clique2.FindClique2;
import chaining.cliquen.FindCliqueN;
import com.google.common.base.Stopwatch;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Created by Thomas on 17/6/2017.
 */
public class Profiling {
    @Test
    public void Test2Clique() {
        final Stopwatch stopwatch = new Stopwatch().start();
        String[] args = {};
        try {
            FindClique2.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Time of execution in milliseconds: " + stopwatch.stop().elapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void TestNClique() {
        final Stopwatch stopwatch = new Stopwatch().start();
        String[] args = {"2"};
        try {
            FindCliqueN.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Time of execution in milliseconds: " + stopwatch.stop().elapsedTime(TimeUnit.MILLISECONDS));
        }
    }
}

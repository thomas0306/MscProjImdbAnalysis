package chaining.clique2;

import actorset.actorsetgenerator.ActorsetGeneratorDriver;
import actorset.initialfiltering.InitialFilteringDriver;
import averaging.AverageDriver;

import namelookup.NameLookupDriver;
import topk.TopKDriver;

/**
 * Created by Thomas on 17/6/2017.
 */
public class FindClique2 {
    public static void main (String[] args) throws Exception {
        // Initial filtering
        String[] InitialFilteringArgs = {"output_datacleaning/", "output_initialfiltering/"};
        InitialFilteringDriver.main(InitialFilteringArgs);

        // Actorset Generation
        String[] ActorsetGeneratorArgs = {"output_initialfiltering/part-r-00000", "output_initialfiltering/", "output_actorset2/"};
        ActorsetGeneratorDriver.main(ActorsetGeneratorArgs);

        // Averaging
        String[] AverageDriverArgs = {"output_datacleaning/ratings-m-00000", "output_actorset2/", "output_avg_actorset2/"};
        AverageDriver.main(AverageDriverArgs);

        // Top K
        String[] TopKDriverArgs = {"output_avg_actorset2/", "output_topk_actorset2/"};
        TopKDriver.main(TopKDriverArgs);

        // Name Lookup
        String[] NameLookupDriverArgs = {"output_datacleaning/actors-m-00000/", "output_topk_actorset2/", "output_topk_name2/"};
        NameLookupDriver.main(NameLookupDriverArgs);
    }
}

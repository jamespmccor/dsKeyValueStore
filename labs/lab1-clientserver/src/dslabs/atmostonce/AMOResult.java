package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Result;
import lombok.Data;

@Data
public final class AMOResult implements Result {
    private final int num;
    private final Result result;
}

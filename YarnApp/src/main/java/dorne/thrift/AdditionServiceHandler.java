package dorne.thrift;

import org.apache.thrift.TException;

/**
 * Created by 1403035 on 2016/6/3.
 */
public class AdditionServiceHandler implements AdditionService.Iface{
    @Override
    public int add(int n1, int n2) throws TException {
        return n1 + n2;
    }
}

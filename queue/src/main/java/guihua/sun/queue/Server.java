package guihua.sun.queue;

/**
 * Created by sunguihua on 2019/7/7.
 */
public interface Server {

    Pointer createIfNull(ConsumerGroup group);

}

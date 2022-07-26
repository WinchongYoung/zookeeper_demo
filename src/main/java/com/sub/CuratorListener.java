package com.sub;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Curator实现监听功能
 * 参考文档：https://www.cnblogs.com/riches/p/15157324.html
 * @date 2022/07/26
 */
public class CuratorListener {

    public static void main(String[] args) throws Exception {

        // 1、在zk创建节点
        String cachePath = "/super/thpffcj";
        //1、创建连接
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(4000, 3);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("127.0.0.1:2183", 4000, 4000, retryPolicy);

        // 2、开启连接
        curatorFramework.start();
        curatorFramework.blockUntilConnected();

        // 3、创建PathChildrenCache【路径监听，用于观察其下子节点变更】【cache有3种：Path Cache、Node Cache、Tree Cache这里只演示其中一种】
        // true代表缓存数据到本地
        PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, cachePath, true);
        // BUILD_INITIAL_CACHE 代表使用同步的方式进行缓存初始化。
        pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        pathChildrenCache.getListenable().addListener((cf, event) -> {
            //4、获取到事件信息，并根据事件类型输出
            PathChildrenCacheEvent.Type eventType = event.getType();
            switch (eventType) {
                case CONNECTION_RECONNECTED:
                    pathChildrenCache.rebuild();
                    System.out.println("重新建立连接！");
                    break;
                case CONNECTION_SUSPENDED:
                    System.out.println("暂停/阻塞连接！");
                    break;
                case CONNECTION_LOST:
                    System.out.println("连接断开！");
                    break;
                case CHILD_ADDED:
                    System.out.println("添加子节点:" + event.getData().getPath());
                    break;
                case CHILD_UPDATED:
                    System.out.println("更新子数据节点:" + new String(event.getData().getData()));
                    break;
                case CHILD_REMOVED:
                    System.out.println("删除子节点:" + event.getData().getPath());
                    break;
                default:
            }
        });

        //5、这里让线程沉睡以等待listener被调用，此时可以直接操作服务器上的节点，在控制台就会打印触发对应事件的内容了
        Thread.sleep(60 * 60 * 1000);

        //6、最后关闭路径监听
        pathChildrenCache.close();
    }

}

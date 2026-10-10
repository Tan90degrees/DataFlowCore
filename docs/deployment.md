# 部署

```bash
docker build -t <registry>/dataflowcore:0.3.0 .
docker push <registry>/dataflowcore:0.3.0
```

镜像包含管控、执行器和示例算子。业务镜像可FROM基础镜像安装业务Python包；依赖
导入后必须验证GIL仍关闭。不在领取任务时pip install。生产使用不可变镜像和匹配
的pool/runtimeVersion。

## K8S

前置：独立持久化PostgreSQL、现有ReadWriteMany PVC、两个不同的至少24字符随机凭证。
PVC在所有Pod挂载/dataflow，UID/GID 10001需要写权限。用权限受控文件创建Secret：

```bash
kubectl create secret generic dataflowcore-secrets \
  --from-file=database-url=/secure/database-url \
  --from-file=admin-token=/secure/admin-token \
  --from-file=worker-token=/secure/worker-token
helm upgrade --install dataflowcore charts/dataflowcore \
  --set image.repository=<registry>/dataflowcore \
  --set image.tag=0.3.0 \
  --set existingDataClaim=<rwx-pvc-name>
kubectl rollout status deployment/dataflowcore-control
kubectl rollout status deployment/dataflowcore-worker
kubectl port-forward service/dataflowcore-control 8080:8080
```

Secret文件不能包含末尾换行。不要把凭证放values.yaml或git中。
管控启动事务化创建schema 1，也可提前运行dataflow migrate。其他schema版本拒绝启动。
Service默认集群内可见；跨网络入口提供TLS，NetworkPolicy限制访问范围。
Pod不挂载service account token，租约同样适用于非K8S环境。

扩容：

```bash
helm upgrade dataflowcore charts/dataflowcore --reuse-values --set worker.replicas=4
```

新增执行器自动领取，不囤积文件。缩容SIGTERM先停止领取再排空；90秒宽限期后未
结束任务由租约重试。资源预算预留解释器和管理进程开销。
升级前备份数据库，暂停新提交，等待完成或安排可接受重试；镜像和runtimeVersion
一起更新。0.3.0沿用schema 1，已有数据保留。旧任务仍绑定原runtimeVersion；升级时保留
旧版本执行器直到排队/重试任务完成，或取消后明确使用新版本重新提交。
每个执行器预建固定数量的常驻槽位，线程上限、回收次数和重资源复用见
[常驻运行进程](resident-runners.md)。
元数据在PostgreSQL，输入/输出在PVC，两者需备份；恢复后过期任务按次数上限重跑。
只剩输出目录无法恢复已丢失的数据库提交语义。

## 文件上传

启用前端上传时管控也需要共享卷写权限；新 Chart 已在 `control.uploadMaxBytes>0` 时启用，设为 0 时恢复只读。Docker Compose 的管控挂载同样已改为可写。前端本身不挂载共享卷，只代理管理 API。

可设置 `control.uploadMaxBytes=1073741824` 与 `control.uploadTimeout=900` 允许最多 1 GiB、900 秒的接收；默认 256 MiB、300 秒、4 个同时上传。外部 Ingress/网关也需同步体积与超时限制。配置、记录备份与清理边界见 [file-uploads.md](file-uploads.md)。

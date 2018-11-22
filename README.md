<h1>项目介绍</h1>
<ol>
<li>基于canal-1.1.1版本改造</li>
<li>支持全量同步（select * from）</li>
<li>为保证数据一致性，全量同步时，增量停止，全量同步后更新binlog position</li>
</ol>


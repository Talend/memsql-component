package com.memsql.talend.components.output.statement;

import com.memsql.talend.components.output.Reject;
import com.memsql.talend.components.service.MemsqlComponentService;
import org.talend.sdk.component.api.record.Record;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

public interface QueryManager extends Serializable {

    List<Reject> execute(List<Record> records, MemsqlComponentService.DataSource dataSource) throws SQLException, IOException;
}

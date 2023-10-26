import { SlotInfo, StatInfo } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';
import Link from 'next/link';

export const SlotTable = ({ data }: { data: SlotInfo[] }) => {
  const getFlowName = (slotName: string) => {
    if (slotName.startsWith('peerflow_slot_')) {
      return slotName.slice(14);
    }
    return '';
  };
  return (
    <>
      <div style={{ fontSize: 17 }}>Replication Slot Information</div>
      <div style={{ maxHeight: '50%', overflow: 'scroll' }}>
        <Table
          header={
            <TableRow>
              <TableCell as='th'>Slot Name</TableCell>
              <TableCell as='th'>Redo LSN</TableCell>
              <TableCell as='th'>Restart LSN</TableCell>
              <TableCell as='th'>Lag (In MB)</TableCell>
            </TableRow>
          }
        >
          {data.map(({ slotName, redoLSN, restartLSN, lagInMb }) => {
            const flowName = getFlowName(slotName);
            return (
              <TableRow key={slotName}>
                <TableCell>
                  {flowName.length >= 1 ? (
                    <Label
                      as={Link}
                      style={{
                        color: 'darkblue',
                        cursor: 'pointer',
                        textDecoration: 'underline',
                      }}
                      href={`/mirrors/edit/${flowName}`}
                    >
                      {slotName}
                    </Label>
                  ) : (
                    { slotName }
                  )}
                </TableCell>
                <TableCell>{redoLSN}</TableCell>
                <TableCell>{restartLSN}</TableCell>
                <TableCell>{lagInMb}</TableCell>
              </TableRow>
            );
          })}
        </Table>
      </div>
    </>
  );
};

export const StatTable = ({ data }: { data: StatInfo[] }) => {
  return (
    <>
      <div style={{ fontSize: 17 }}>Stat Activity Information</div>
      <div style={{ maxHeight: '50%', overflow: 'scroll' }}>
        <Table
          header={
            <TableRow>
              <TableCell as='th'>PID</TableCell>
              <TableCell as='th'>Duration</TableCell>
              <TableCell as='th'>Query</TableCell>
            </TableRow>
          }
        >
          {data.map(({ pid, duration, query }) => (
            <TableRow key={pid}>
              <TableCell>{pid}</TableCell>
              <TableCell>
                {duration >= 3600
                  ? `${Math.floor(duration / 3600)} hour(s) ${Math.floor(
                      (duration % 3600) / 60
                    )} minutes`
                  : duration >= 60
                  ? `${Math.floor(duration / 60)} minutes ${Math.floor(
                      duration % 60
                    )} seconds`
                  : `${duration.toFixed(2)} seconds`}
              </TableCell>
              <TableCell>
                <div
                  style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    fontFamily: 'monospace',
                    fontSize: 15,
                  }}
                >
                  {query}
                  <Button>
                    <Icon name='content_copy' />
                  </Button>
                </div>
              </TableCell>
            </TableRow>
          ))}
        </Table>
      </div>
    </>
  );
};
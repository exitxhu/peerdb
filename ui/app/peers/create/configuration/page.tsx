'use client';
import { Button } from '@/lib/Button';
import { ButtonGroup } from '@/lib/ButtonGroup';
import { Label } from '@/lib/Label';
import { LayoutMain, RowWithTextField } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { TextField } from '@/lib/TextField';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';
import { useState } from 'react';
import PgConfig from './configForm';
import { handleCreate, handleValidate } from './handlers';
import { postgresSetting } from './helpers/pg';
import { PeerConfig } from './types';

export default function CreateConfig() {
  const searchParams = useSearchParams();
  const dbType = searchParams.get('dbtype') || '';
  const [name, setName] = useState<string>('');
  const [config, setConfig] = useState<PeerConfig>({
    host: '',
    port: 5432,
    user: '',
    password: '',
    database: '',
    transactionSnapshot: '',
  });
  const [formMessage, setFormMessage] = useState<{ ok: boolean; msg: string }>({
    ok: true,
    msg: '',
  });
  const [loading, setLoading] = useState<boolean>(false);
  const configComponentMap = (dbType: string) => {
    switch (dbType) {
      case 'POSTGRES':
        return <PgConfig settings={postgresSetting} setter={setConfig} />;
      default:
        return <></>;
    }
  };

  return (
    <LayoutMain alignSelf='center' justifySelf='center' width='xxLarge'>
      <Panel>
        <Label variant='title3'>New peer</Label>
        <Label colorName='lowContrast'>Set up a new peer.</Label>
      </Panel>
      <Panel>
        <Label colorName='lowContrast' variant='subheadline'>
          Details
        </Label>
        <RowWithTextField
          label={<Label as='label'>Name</Label>}
          action={
            <TextField
              variant='simple'
              onChange={(e) => setName(e.target.value || '')}
            />
          }
        />
        {dbType && configComponentMap(dbType)}
      </Panel>
      <Panel>
        <ButtonGroup>
          <Button as={Link} href='/peers/create'>
            Back
          </Button>
          <Button
            style={{ backgroundColor: 'gold' }}
            onClick={() =>
              handleValidate(dbType, config, setFormMessage, setLoading, name)
            }
          >
            Validate
          </Button>
          <Button
            variant='normalSolid'
            onClick={() =>
              handleCreate(dbType, config, setFormMessage, setLoading, name)
            }
          >
            Create
          </Button>
        </ButtonGroup>
        <Panel>
          {loading && (
            <Label
              colorName='lowContrast'
              colorSet='base'
              variant='subheadline'
            >
              Validating...
            </Label>
          )}
          {!loading && formMessage.msg.length > 0 && (
            <Label
              colorName='lowContrast'
              colorSet={formMessage.ok ? 'positive' : 'destructive'}
              variant='subheadline'
            >
              {formMessage.msg}
            </Label>
          )}
        </Panel>
      </Panel>
    </LayoutMain>
  );
}

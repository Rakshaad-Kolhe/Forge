import { describe, it, expect } from 'vitest';
import {
  createJobAttemptStateMachine,
  createJobStateMachine,
  createPipelineRunStateMachine,
} from './state-machine.js';
import { InvalidStateTransitionError } from './errors.js';

describe('State Machines', () => {
  describe('PipelineRunStateMachine', () => {
    it('executes legal happy-path lifecycle (PENDING -> QUEUED -> RUNNING -> SUCCEEDED)', () => {
      const sm = createPipelineRunStateMachine('run-1');
      expect(sm.getStatus()).toBe('PENDING');

      sm.transitionTo('QUEUED');
      expect(sm.getStatus()).toBe('QUEUED');

      sm.transitionTo('RUNNING');
      expect(sm.getStatus()).toBe('RUNNING');

      sm.transitionTo('SUCCEEDED');
      expect(sm.getStatus()).toBe('SUCCEEDED');
      expect(sm.isTerminal()).toBe(true);
    });

    it('executes legal failure transition (RUNNING -> FAILED)', () => {
      const sm = createPipelineRunStateMachine('run-2');
      sm.transitionTo('QUEUED');
      sm.transitionTo('RUNNING');
      sm.transitionTo('FAILED');
      expect(sm.getStatus()).toBe('FAILED');
      expect(sm.isTerminal()).toBe(true);
    });

    it('executes legal cancellation from PENDING, QUEUED, and RUNNING', () => {
      const sm1 = createPipelineRunStateMachine('run-c1', 'PENDING');
      sm1.transitionTo('CANCELLED');
      expect(sm1.getStatus()).toBe('CANCELLED');

      const sm2 = createPipelineRunStateMachine('run-c2', 'QUEUED');
      sm2.transitionTo('CANCELLED');
      expect(sm2.getStatus()).toBe('CANCELLED');

      const sm3 = createPipelineRunStateMachine('run-c3', 'RUNNING');
      sm3.transitionTo('CANCELLED');
      expect(sm3.getStatus()).toBe('CANCELLED');
    });

    it('executes legal timeout transition (RUNNING -> TIMED_OUT)', () => {
      const sm = createPipelineRunStateMachine('run-t', 'RUNNING');
      sm.transitionTo('TIMED_OUT');
      expect(sm.getStatus()).toBe('TIMED_OUT');
      expect(sm.isTerminal()).toBe(true);
    });

    it('rejects skipping state (PENDING -> RUNNING)', () => {
      const sm = createPipelineRunStateMachine('run-skip');
      expect(() => sm.transitionTo('RUNNING')).toThrow(InvalidStateTransitionError);
    });

    it('rejects transition from terminal state (SUCCEEDED -> RUNNING)', () => {
      const sm = createPipelineRunStateMachine('run-term', 'SUCCEEDED');
      expect(() => sm.transitionTo('RUNNING')).toThrow(InvalidStateTransitionError);
    });

    it('rejects transition from FAILED to QUEUED', () => {
      const sm = createPipelineRunStateMachine('run-failed', 'FAILED');
      expect(() => sm.transitionTo('QUEUED')).toThrow(InvalidStateTransitionError);
    });

    it('is a no-op when re-transitioning to current state', () => {
      const sm = createPipelineRunStateMachine('run-same', 'RUNNING');
      expect(() => sm.transitionTo('RUNNING')).not.toThrow();
      expect(sm.getStatus()).toBe('RUNNING');
    });
  });

  describe('JobStateMachine', () => {
    it('executes legal lifecycle (PENDING -> QUEUED -> RUNNING -> SUCCEEDED)', () => {
      const sm = createJobStateMachine('job-1');
      sm.transitionTo('QUEUED');
      sm.transitionTo('RUNNING');
      sm.transitionTo('SUCCEEDED');
      expect(sm.getStatus()).toBe('SUCCEEDED');
      expect(sm.isTerminal()).toBe(true);
    });

    it('rejects terminal state regression (FAILED -> RUNNING)', () => {
      const sm = createJobStateMachine('job-term', 'FAILED');
      expect(() => sm.transitionTo('RUNNING')).toThrow(InvalidStateTransitionError);
    });

    it('rejects direct PENDING to SUCCEEDED', () => {
      const sm = createJobStateMachine('job-invalid');
      expect(() => sm.transitionTo('SUCCEEDED')).toThrow(InvalidStateTransitionError);
    });
  });

  describe('JobAttemptStateMachine', () => {
    it('executes legal attempt lifecycle (PENDING -> RUNNING -> SUCCEEDED)', () => {
      const sm = createJobAttemptStateMachine('att-1');
      sm.transitionTo('RUNNING');
      sm.transitionTo('SUCCEEDED');
      expect(sm.getStatus()).toBe('SUCCEEDED');
      expect(sm.isTerminal()).toBe(true);
    });

    it('rejects attempt transition once terminal', () => {
      const sm = createJobAttemptStateMachine('att-term', 'CANCELLED');
      expect(() => sm.transitionTo('RUNNING')).toThrow(InvalidStateTransitionError);
    });
  });
});
